from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter((F.col("segment") == "Consumer") & F.col("is_active") & (F.col("signup_date") <= "2023-12-27"))
    .withColumn("name_segment_key", F.concat_ws("/", "name", "segment"))
)

agg = (
    recent_customers
    .groupBy("is_active")
    .agg(
        F.sum(F.when(F.col("country") == "UK", F.col("age")).otherwise(0)).alias("uk_age"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
)

result = (
    agg
    .filter(F.col("uk_age") > 57)
    .select("is_active", "avg_credit_limit")
)
