from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["DE", "US"]))
    .groupBy("is_active")
    .agg(
        F.sum(F.when(F.col("country") == "US", F.col("credit_limit")).otherwise(0)).alias("us_credit_limit"),
    )
    .filter(F.col("us_credit_limit") < 66)
    .orderBy(F.col("us_credit_limit").desc())
)
