from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 15600, "PREMIUM").otherwise("STANDARD"))
    .rollup("credit_limit_tier", "country")
    .agg(
        F.percentile_approx("age", 0.95).alias("age_p95"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("credit_limit")).otherwise(0)).alias("smb_credit_limit"),
        F.min("age").alias("min_age"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.countDistinct("country").alias("distinct_countries"),
        F.grouping("credit_limit_tier").alias("grouping_level"),
    )
)
