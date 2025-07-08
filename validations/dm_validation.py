from pyspark.sql import SparkSession
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

spark = SparkSession.builder.appName("Validation").getOrCreate()
df = spark.read.parquet("s3://clinical-processed/sdtm/dm/")

check = Check(spark, CheckLevel.Error, "DM Validation")
check_result = VerificationSuite(spark).onData(df)\
    .addCheck(check.hasSize(lambda x: x > 0)
    .isComplete("USUBJID")
    .isContainedIn("SEX", ["M", "F"]))\
    .run()
