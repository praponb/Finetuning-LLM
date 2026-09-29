from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

base = (
    flattened
    .filter(F.col("created_at") >= F.lit("2025-03-02 00:00:00").cast("timestamp"))
    .withColumn("is_gift", F.when(F.col("key") == "gift", 1).otherwise(0))
)

valid = (
    base
    .filter(F.col("key").isNotNull())
    .select(
        "id",
        "created_at",
        "key",
        "v",
        F.col("is_gift").cast("string").alias("is_gift_str"),
        F.array_contains("coupon_codes", "WELCOME5").alias("is_welcome5"),
        F.coalesce("key", F.lit("DIRECT")).alias("key_filled"),
    )
)

filtered = (
    valid
    .filter(F.col("key").isNotNull() & (~F.col("key").eqNullSafe(F.lit("device"))))
    .select(
        "id",
        "created_at",
        "key",
        "is_gift_str",
        "is_welcome5",
        F.when(F.col("is_welcome5"), "Yes").otherwise("No").alias("is_welcome5_label"),
        F.coalesce("key", F.lit("OTHER")).alias("key_clean"),
    )
)

cleaned = (
    filtered
    .filter(F.col("key").isNotNull() & (F.col("created_at") >= F.to_date(F.lit("2025-03-01"))))
    .withColumn("is_welcome5_label_2", F.when(F.col("is_welcome5"), "Yes").otherwise("No"))
)

result = (
    cleaned
    .select(
        "id",
        "created_at",
        "key",
        "is_gift_str",
        "is_welcome5",
        "is_welcome5_label",
        "key_clean",
        "is_welcome5_label_2",
        F.when(F.col("key").rlike("^(utm_source|gift)$"), "GROUP_A").when(F.col("key").rlike("^(device|weight_kg)$"), "CORE").otherwise("GROUP_B").alias("key_zone"),
    )
    .orderBy(F.col("created_at").desc_nulls_first())
)
