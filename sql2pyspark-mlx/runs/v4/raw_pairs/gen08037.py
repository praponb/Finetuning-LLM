from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 16900, "LARGE").otherwise("SMALL").alias("credit_limit_tier"),
    )
    .agg(
        F.min("age").alias("min_age"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
        F.countDistinct("country").alias("distinct_countries"),
    )
    .filter(F.col("min_age") >= 0)
    .orderBy(F.col("credit_limit_tier").asc_nulls_last())
)
