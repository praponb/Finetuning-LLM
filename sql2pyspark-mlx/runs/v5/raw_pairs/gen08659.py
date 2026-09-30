from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

exploded = lines.select("id", "created_at", "key", F.explode("coupon_codes").alias("value_2"))

cleaned = (
    exploded
    .filter(F.to_date("created_at") >= "2025-09-01")
    .select(
        "*",
        F.date_format("created_at", "yyyy-MM-dd").alias("created_at_label"),
        F.coalesce("value_2", F.lit("UNKNOWN")).alias("value_2_clean"),
    )
)

filtered = (
    cleaned
    .filter(F.col("value_2").isNotNull())
    .select(
        "id",
        "created_at",
        "key",
        "created_at_label",
        "value_2_clean",
        F.trim("key").alias("clean_key"),
    )
)

cleaned_carts = (
    filtered
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("key").isNotNull())
    .select(
        "id",
        "created_at",
        "key",
        "created_at_label",
        "value_2_clean",
        "clean_key",
        F.upper("key").alias("key_upper"),
        F.date_format("created_at", "yyyy-MM-dd").alias("created_at_label_2"),
        F.coalesce("key", F.lit("OTHER")).alias("key_filled"),
    )
)

result = (
    cleaned_carts
    .groupBy("key")
    .agg(
        F.max("created_at").alias("last_created_at"),
        F.countDistinct("id").alias("distinct_ids"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes_2"),
        F.count(F.when(F.col("key") == "gift", 1)).alias("gift_count"),
        F.count(F.when(F.col("key") == "gift", 1)).alias("gift_rows"),
    )
    .orderBy(F.col("span_minutes").desc())
)
