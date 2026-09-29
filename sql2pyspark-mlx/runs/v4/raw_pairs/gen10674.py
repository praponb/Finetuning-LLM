from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city", "is_active")
    .agg(
        F.sum(F.when(F.col("segment") == "Consumer", F.col("credit_limit")).otherwise(0)).alias("consumer_credit_limit"),
        F.countDistinct("age").alias("unique_countrys"),
    )
    .filter(F.col("unique_countrys") >= 8)
    .select("city", "is_active", "consumer_credit_limit", "unique_countrys")
)
