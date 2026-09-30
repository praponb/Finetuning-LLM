from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "country",
        F.when(F.col("credit_limit") >= 12900, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.min("credit_limit").alias("min_credit_limit"),
        F.percentile_approx("credit_limit", 0.95).alias("credit_limit_p95"),
    )
    .orderBy(F.col("country").desc_nulls_first())
)
