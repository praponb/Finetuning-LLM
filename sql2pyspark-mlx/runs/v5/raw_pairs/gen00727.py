from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

flattened = exploded.select("key", "attr_value", F.explode("coupon_codes").alias("tag"))

filtered = (
    flattened
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.lower("tag").alias("tag_lower"),
        F.coalesce("key", F.lit("UNKNOWN")).alias("key_filled"),
    )
)

cleaned = (
    filtered
    .filter(F.col("key") == "device")
    .select(
        "key",
        "attr_value",
        "tag",
        "tag_lower",
        "key_filled",
        F.trim("tag").alias("clean_tag"),
    )
)

base = (
    cleaned
    .filter(F.col("tag").isNotNull() & (F.col("tag") == "VIP"))
    .select(
        "key",
        "tag_lower",
        "key_filled",
        "clean_tag",
        F.initcap(F.trim("tag")).alias("tag_title"),
        F.concat("tag", F.lit(" - "), "key").alias("tag_key"),
        F.coalesce("key", F.lit("OTHER")).alias("key_clean"),
    )
)

result = (
    base
    .select(
        "key",
        "tag_lower",
        "key_filled",
        "tag_title",
        "key_clean",
        F.when(F.col("key").rlike("^(device|gift)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("key_zone"),
    )
    .orderBy(F.col("key_zone").desc(), "key")
)
