from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("age") >= 62, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
    )
    .agg(
        F.bit_or("age").alias("age_bit_or"),
        F.percentile_approx("age", 0.95).alias("age_p95"),
    )
)
