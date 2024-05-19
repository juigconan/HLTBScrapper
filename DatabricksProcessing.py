import re
from pyspark.sql.functions import col, split, udf
from pyspark.sql.types import IntegerType, DoubleType, StringType
regexTime = "(<\/?h5>)"
regexNameStart = '">'
regexNameFinish = '</'
replaceList = {"&amp;":"&","&lt;":"<","&gt;":">","&apos;":"'","&cuot;":"\"","<!-- -->":""}
# Cambiar a las distintas cuentas de almacenamiento cuando las tenga
timeRawFile = "dbfs:/FileStore/HLTB/scrapped_data_time.csv"
nameRawFile = "dbfs:/FileStore/HLTB/scrapped_data_name.csv"
sinkFile = "dbfs:/FileStore/HLTB/scrapped_data_complete.csv"
def cleanName(row):
    newRow = re.split(regexNameFinish, re.split(regexNameStart, row)[1])[0]
    for key in replaceList:
        newRow = newRow.replace(key, replaceList[key])
    return newRow

udfCleanName = udf(lambda row: cleanName(row), StringType())
udfCleanHours = udf(lambda row: re.split(" ", row)[0].replace("½", ".5"), StringType())

dfTimeRaw = spark.read.option("header", "true").csv(timeRawFile).drop("_c0")
dfTimeProcessed = dfTimeRaw.withColumns({"hltbIndex": col("hltbIndex").cast(IntegerType()),"Main":split(col("Main"),regexTime)[1],"Main + Sides":split(col("Main + Sides"),regexTime)[1],"Completionist":split(col("Completionist"),regexTime)[1],"All Styles":split(col("All Styles"),regexTime)[1]})
dfNameRaw = spark.read.option("header", "true").csv(nameRawFile).drop("_c0")
dfNameProcessed = dfNameRaw.withColumns({"hltbIndex":col("0_x").cast(IntegerType()),"Name": udfCleanName("0_y")}).select("hltbIndex", "Name")
dfFinal = dfNameProcessed.join(dfTimeProcessed, "hltbIndex").withColumns({"Main": udfCleanHours("Main").cast(DoubleType()),"Main + Sides":udfCleanHours("Main + Sides").cast(DoubleType()),"Completionist":udfCleanHours("Completionist").cast(DoubleType()),"All Styles":udfCleanHours("All Styles").cast(DoubleType())})
dfFinal.repartition(1).write.mode("overwrite").format('com.databricks.spark.csv').save(sinkFile,header = 'true')