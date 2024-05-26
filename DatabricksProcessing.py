# Databricks notebook source
import re
from pyspark.sql.functions import col, split, udf, explode, col
from pyspark.sql.types import IntegerType, DoubleType, StringType
from time import sleep

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
dbTable = dbutils.secrets.get(scope="scopegamechoice",key="dbTable")
dbUser = dbutils.secrets.get(scope="scopegamechoice",key="dbUser")
dbPass = dbutils.secrets.get(scope="scopegamechoice",key="dbPass")

# COMMAND ----------

# Con todo este bloque hacemos que se guarde un archivo único en nuestro blob
spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("parquet.enable.summary-metadata", "false")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

tempPath = prefixProcessed % "temp.csv"

def saveDf(df,saveRoute):
    df.repartition(1).write.mode("overwrite").format('com.databricks.spark.csv').save(saveRoute,header = 'true')
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

# Dataframe con horas y nombre
dfHltb = dfNameProcessed.join(dfTimeProcessed, "hltbIndex").withColumns({"Main": udfCleanHours("Main").cast(DoubleType()),"Main + Sides":udfCleanHours("Main + Sides").cast(DoubleType()),"Completionist":udfCleanHours("Completionist").cast(DoubleType()),"All Styles":udfCleanHours("All Styles").cast(DoubleType())})
saveDf(dfHltb, hltbSinkFile)
dfHltbProcessed = spark.read.option("header", "true").format("csv").load(hltbSinkFile)

# Dataframe con el join final
dfFinal = dfSteamGames.join(dfHltbProcessed, "Name").select("hltbIndex", "steamIndex", "Name", "Main", "Main + Sides", "Completionist", "All Styles")

dfFinal.write.format('jdbc').options( \
      url=dbUrl, \
      database=dbName, \
      dbtable=dbTable, \
      user=dbUser, \
      password=dbPass).mode('overwrite').save()