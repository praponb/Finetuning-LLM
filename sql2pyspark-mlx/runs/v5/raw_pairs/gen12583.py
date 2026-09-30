from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-02-26").cast("date")) & (F.length("name") >= 8))
    .select(
        "*",
        F.when(F.col("country").isin(["SG", "US"]), "Priority").otherwise("Other").alias("country_group"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_clean"),
    )
)

result = (
    base
    .withColumn("quarter_start", F.date_trunc("QUARTER", "signup_date"))
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 14600, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["quarter_start", "credit_limit_tier"], ["quarter_start"]],
        "quarter_start",
        "credit_limit_tier",
    )
    .agg(
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.grouping_id().alias("grouping_level"),
    )
)
