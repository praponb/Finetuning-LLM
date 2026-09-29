from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active"))
    .groupBy("city", "segment")
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.round(((100 * F.sum(F.when(F.col("segment") == "Consumer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_consumer"),
    )
)
