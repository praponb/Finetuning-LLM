from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", "city")
    .agg(
        F.min("age").alias("min_age"),
        F.round(((100 * F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0))) / F.count("*")), 1).alias("pct_corporate"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("age")).otherwise(0)).alias("corporate_age"),
    )
)
