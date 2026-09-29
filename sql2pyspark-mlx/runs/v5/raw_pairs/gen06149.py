from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

cleaned = (
    lines
    .filter(F.col("attr_key").isNotNull())
    .withColumn("attr_key_lower", F.lower("attr_key"))
)

base = (
    cleaned
    .filter(F.col("attr_key").isin(["utm_source", "device", "weight_kg"]) & (F.col("created_at") > F.to_date(F.lit("2025-03-02"))))
    .withColumn("created_at_label", F.date_format("created_at", "MMM yyyy"))
    .withColumn("resolved_attr_key", F.coalesce("attr_key", F.lit("NONE")))
)

filtered = (
    base
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & (~F.col("attr_key").eqNullSafe(F.lit("utm_source"))))
    .select(
        "*",
        F.date_trunc("month", "created_at").alias("created_at_period"),
        F.coalesce("attr_key", F.lit("Unknown")).alias("attr_key_filled"),
        F.coalesce("attr_key", F.lit("UNKNOWN")).alias("resolved_attr_key_2"),
    )
)

result = (
    filtered
    .groupBy("attr_key")
    .agg(
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_rows"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_rows_2"),
        F.count(F.when(F.col("attr_key") == "device", 1)).alias("device_rows"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.min("created_at").alias("first_seen"),
    )
    .orderBy(F.col("num_attr_keys").desc_nulls_first())
)
