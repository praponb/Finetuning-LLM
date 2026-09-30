from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

lines = exploded.select("id", "created_at", "key", F.explode("coupon_codes").alias("value"))

base = (
    lines
    .filter((F.col("created_at") >= F.lit("2025-03-02 00:00:00").cast("timestamp")) & (F.col("key").eqNullSafe(F.lit("utm_source"))))
    .select(
        "*",
        F.date_trunc("week", "created_at").alias("created_at_period"),
        F.date_trunc("week", "created_at").alias("created_at_period_2"),
    )
)

cleaned = (
    base
    .filter(F.col("value").isNotNull())
    .withColumn("created_at_day", F.col("created_at").cast("date"))
    .withColumn("days_since_created_at_period", F.datediff(F.current_date(), "created_at_period"))
)

result = (
    cleaned
    .groupBy("value")
    .agg(
        F.sum(F.when(F.col("value") == "BUNDLE", F.col("days_since_created_at_period"))).alias("bundle_total_days_since_created_at_period"),
        F.max("created_at_period").alias("last_created_at_period"),
        F.round(F.avg("days_since_created_at_period"), 2).alias("avg_days_since_created_at_period"),
    )
    .orderBy(F.col("value").desc_nulls_last())
)
