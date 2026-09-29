from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active", "country")
    .agg(
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
    .orderBy(F.col("consumer_count").desc())
)
