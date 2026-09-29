from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Taylo%") & (F.col("signup_date") >= "2024-12-01"))
    .groupBy(
        F.when(F.col("segment").isin(["SMB", "Consumer"]), "tier_1").otherwise("other").alias("segment_group"),
    )
    .agg(
        F.min("signup_date").alias("min_signup_date"),
        F.max("signup_date").alias("latest_signup_date"),
        F.round(((100 * F.sum(F.when(F.col("segment") == "Consumer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_consumer"),
    )
)
