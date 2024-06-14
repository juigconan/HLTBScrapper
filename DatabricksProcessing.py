# Databricks notebook source
import re
from pyspark.sql.functions import col, split, udf, explode, col
from pyspark.sql.types import IntegerType, DoubleType, StringType
from time import sleep
from bs4 import BeautifulSoup
import requests

# COMMAND ----------

# Configuración conexion con Azure
service_credential = dbutils.secrets.get(scope="scopegamechoice",key="blobsecret")
blob = dbutils.secrets.get(scope="scopegamechoice",key="blob")
tennant = dbutils.secrets.get(scope="scopegamechoice",key="tennant")
app = dbutils.secrets.get(scope="scopegamechoice",key="app")

spark.conf.set(f"fs.azure.account.auth.type.{blob}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{blob}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{blob}.dfs.core.windows.net", app)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{blob}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{blob}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tennant}/oauth2/token")

prefixRaw = f"abfss://raw@{blob}.dfs.core.windows.net/%s" % "%s"
prefixProcessed = f"abfss://processed@{blob}.dfs.core.windows.net/%s" % "%s"

# Configuración conexion base de datos
dbUrl = dbutils.secrets.get(scope="scopegamechoice",key="dbUrl")
dbName = dbutils.secrets.get(scope="scopegamechoice",key="dbName")
dbTableFinal = dbutils.secrets.get(scope="scopegamechoice",key="dbTableFinal")
dbTableHltb = dbutils.secrets.get(scope="scopegamechoice",key="dbTableHltb")
dbUser = dbutils.secrets.get(scope="scopegamechoice",key="dbUser")
dbPass = dbutils.secrets.get(scope="scopegamechoice",key="dbPass")

# COMMAND ----------

# Con todo este bloque hacemos que se guarde un archivo único en nuestro blob
spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("parquet.enable.summary-metadata", "false")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

tempPath = prefixProcessed % "temp.csv"

def saveDf(df,saveRoute, mode='csv'):
    if (mode == 'csv'):
        df.repartition(1).write.mode("overwrite").format('com.databricks.spark.csv').save(saveRoute,header = 'true')
    if (mode == 'json'):
        df.repartition(1).write.mode("overwrite").format('json').save(saveRoute)
    sleep(0.2)
    file = re.search("part([^'])*",(dbutils.fs.ls(saveRoute)[0])[0])[0]
    loadPoint = "%s/%s" % (saveRoute, file)
    dbutils.fs.mv(loadPoint, tempPath,True)
    dbutils.fs.rm(saveRoute)
    dbutils.fs.mv(tempPath, saveRoute)

# COMMAND ----------

# Variables para las expresiones regulares
regexTime = "(<\/?h5>)"
regexNameStart = '">'
regexNameFinish = '</'
replaceList = {"&amp;":"&","&lt;":"<","&gt;":">","&apos;":"'","&cuot;":"\"","<!-- -->":""}

# Path a los archivos
timeRawFile = prefixRaw % "scrapped_data_time.csv"
nameRawFile = prefixRaw % "scrapped_data_name.csv"
steamRawFile = prefixRaw % "steam_games.json"
hltbSinkFile = prefixProcessed % "scrapped_data_complete.csv"
hltbImageRawFile = prefixRaw % "scrapped_data_image.csv"
hltbImageSinkFile = prefixProcessed % "scrapped_data_complete_image.csv"
steamSinkFile = prefixProcessed % "steam_games_processed.json"

# COMMAND ----------

# Función para limpiar el nombre
def cleanName(row):
    newRow = re.split(regexNameFinish, re.split(regexNameStart, row)[1])[0]
    for key in replaceList:
        newRow = newRow.replace(key, replaceList[key])
    return newRow

udfCleanName = udf(lambda row: cleanName(row), StringType())

# Función para limpiar las horas
udfCleanHours = udf(lambda row: re.split(" ", row)[0].replace("½", ".5"), StringType())

# Dataframes con las horas
dfTimeRaw = spark.read.option("header", "true").format("csv").load(timeRawFile).drop("_c0")
dfTimeProcessed = dfTimeRaw.withColumns({"hltbIndex": col("hltbIndex").cast(IntegerType()),"Main":split(col("Main"),regexTime)[1],"Main + Sides":split(col("Main + Sides"),regexTime)[1],"Completionist":split(col("Completionist"),regexTime)[1],"All Styles":split(col("All Styles"),regexTime)[1]})

# Dataframes con el nombre
dfNameRaw = spark.read.option("header", "true").format("csv").load(nameRawFile).drop("_c0")
dfNameProcessed = dfNameRaw.withColumns({"hltbIndex":col("0_x").cast(IntegerType()),"Name": udfCleanName("0_y")}).select("hltbIndex", "Name")

# Dataframe de Steam
dfSteamGames = spark.read.option("header", "true").format("json").load(steamRawFile).select("applist.*").withColumn("apps", explode("apps")).select("apps.*").withColumnsRenamed({"name": "Name", "appid": "steamIndex"})
saveDf(dfSteamGames, steamSinkFile, "json")

# Dataframe con horas y nombre
dfHltb = dfNameProcessed.join(dfTimeProcessed, "hltbIndex").withColumns({"Main": udfCleanHours("Main").cast(DoubleType()),"Main + Sides":udfCleanHours("Main + Sides").cast(DoubleType()),"Completionist":udfCleanHours("Completionist").cast(DoubleType()),"All Styles":udfCleanHours("All Styles").cast(DoubleType())}).select("*")
saveDf(dfHltb, hltbSinkFile)
dfHltbProcessed = spark.read.option("header", "true").format("csv").load(hltbSinkFile).withColumns({"Main": col("Main").cast(DoubleType()),"Main + Sides": col("Main + Sides").cast(DoubleType()),"Completionist": col("Completionist").cast(DoubleType()),"All Styles": col("All Styles").cast(DoubleType())})

# Dataframe con el join final
dfFinal = dfSteamGames.join(dfHltbProcessed, "Name").select("hltbIndex", "steamIndex", "Name", "Main", "Main + Sides", "Completionist", "All Styles")

# Guardamos los datos en la base de datos
dfFinal.write.format('jdbc').options( \
      url=dbUrl, \
      database=dbName, \
      dbtable=dbTableFinal, \
      user=dbUser, \
      password=dbPass).mode('overwrite').save()


# COMMAND ----------

def cleanImage(row):
    try:
        newRow = re.split("\"/>", re.split("src=\"", row)[1])[0]
    except:
        newRow = ""
    for key in replaceList:
        newRow = newRow.replace(key, replaceList[key])
    newRow = newRow.replace("\"", "")
    return newRow

udfCleanImage =  udf(lambda row: cleanImage(row), StringType())

dfProcessed = spark.read.option("header", "true").format("csv").load(hltbSinkFile)
dfImagen = spark.read.option("header", "true").format("csv").load(hltbImageRawFile).drop("_c0").withColumn("Image", udfCleanImage(col("imageSrc"))).drop("imageSrc")

dfProcessedImagen = dfProcessed.join(dfImagen, "hltbIndex", "left").withColumns({"hltbIndex": col("hltbIndex").cast(IntegerType()),"Main": col("Main").cast(DoubleType()),"Main + Sides": col("Main + Sides").cast(DoubleType()),"Completionist": col("Completionist").cast(DoubleType()),"All Styles": col("All Styles").cast(DoubleType())})

# COMMAND ----------

dfProcessedImagen.write.format('jdbc').options( \
      url=dbUrl, \
      database=dbName, \
      dbtable=dbTableHltb, \
      user=dbUser, \
      password=dbPass).mode('overwrite').save()

saveDf(dfProcessedImagen,hltbImageSinkFile)
