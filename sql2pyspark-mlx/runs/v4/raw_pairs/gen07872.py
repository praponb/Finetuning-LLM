from pyspark.sql import functions as F

result = (
    df_customers
    .rollup("country")
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.percentile_approx("credit_limit", 0.95).alias("credit_limit_p95"),
        F.grouping("country").alias("grouping_id"),
    )
)
