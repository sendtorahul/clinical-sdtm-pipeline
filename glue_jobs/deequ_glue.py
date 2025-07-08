import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# Deequ imports
from pydeequ.checks import *
from pydeequ.verification import VerificationSuite
from pydeequ.verification import VerificationResult
from pydeequ.repository import FileSystemMetricsRepository, ResultKey

# Initialize Spark + Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Load clinical data from S3
input_path = "s3://your-bucket/raw/clinical_data.csv"
df = spark.read.option("header", True).csv(input_path)

# Apply PyDeequ checks
check = Check(spark, CheckLevel.Error, "Clinical Data Quality Checks")

check_result = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(
        check
        .isComplete("subject_id")               # Not null
        .isUnique("subject_id")                 # Unique
        .isComplete("age")                      # Age not null
        .isContainedIn("gender", ["M", "F"])    # Gender must be M/F
        .isBetween("age", lowerBound=18, upperBound=100, inclusive=True)  # Age range
    ) \
    .run()

# Save result to S3 in JSON format
result_df = VerificationResult.checkResultsAsDataFrame(spark, check_result)
output_path = "s3://your-bucket/results/clinical_data_quality.json"
result_df.coalesce(1).write.mode("overwrite").json(output_path)
