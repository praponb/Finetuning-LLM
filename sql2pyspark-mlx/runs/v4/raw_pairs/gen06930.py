from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("value"))
)

valid = (
    flattened
    .filter((F.col("created_at") > F.lit("2025-03-01 17:00:00").cast("timestamp")) & F.col("value").isNotNull() & (F.col("created_at") < "2024-11-01"))
    .withColumn("created_at_hour", F.hour("created_at"))
    .withColumn("created_at_month", F.month("created_at"))
)

matched_keys = (
    valid
    .select("customer_id", "id")
    .exceptAll(
        df_carts
        .filter(F.col("created_at") < "2024-01-01")
        .select("customer_id", "id")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid_carts = (
    verified
    .filter((F.col("created_at") >= F.lit("2025-03-02 12:00:00").cast("timestamp")) & F.col("value").isNotNull())
    .withColumn("days_since_created_at", F.datediff(F.current_date(), "created_at"))
    .withColumn("resolved_value", F.coalesce("value", F.lit("NONE")))
)

result = (
    valid_carts
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.sort_array(F.collect_set("value")).alias("value_list"),
        F.avg(F.when(F.col("value") == "BUNDLE", F.col("created_at_month"))).alias("bundle_avg_created_at_month"),
    )
)
