from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0)).alias("corporate_count"),
    )
    .filter(F.col("corporate_count") < 32)
)
