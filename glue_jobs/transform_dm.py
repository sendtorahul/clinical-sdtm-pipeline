from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

spark = SparkSession.builder.appName("SDTM_DM_Transform").getOrCreate()

# Read raw data
df = spark.read.option("header", True).csv("s3://clinical-raw-zone/study123/raw/dm.csv")

# Clean & Transform
df_clean = (
    df.dropDuplicates(["SUBJID"])
      .withColumn("BRTHDTC", trim(col("BRTHDTC")))
      .filter(col("AGE").isNotNull())
)

# SDTM Mapping
sdtm_map = {
    "SUBJID": "USUBJID",
    "BRTHDTC": "BRTHDTC",
    "AGE": "AGE",
    "SEX": "SEX"
}

df_sdtm = df_clean.selectExpr([f"{k} as {v}" for k, v in sdtm_map.items()])

# Save
output_path = "s3://clinical-processed/sdtm/dm/"
df_sdtm.write.mode("overwrite").parquet(output_path)

print(f"✅ SDTM DM domain written to {output_path}")
