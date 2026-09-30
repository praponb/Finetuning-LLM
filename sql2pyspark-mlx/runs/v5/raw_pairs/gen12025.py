from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("tag"))

base = (
    exploded
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("tag").isNotNull())
    .select(
        "*",
        F.lower("tag").alias("tag_lower"),
        F.when(F.col("tag").isin(["SPRING10", "WELCOME5"]), "Core").otherwise("Other").alias("tag_group"),
        F.coalesce("tag", F.lit("OTHER")).alias("tag_filled"),
    )
)

valid = (
    base
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 22:00:00"))) & (F.col("created_at") >= "2024-05-13"))
    .select(
        "*",
        F.lower("tag").alias("tag_lower_2"),
        F.coalesce("tag", F.lit("DIRECT")).alias("tag_filled_2"),
    )
)

filtered = (
    valid
    .filter(F.col("created_at") > F.to_date(F.lit("2025-03-01")))
    .select(
        "id",
        "created_at",
        "tag",
        "tag_lower",
        "tag_group",
        "tag_lower_2",
        "tag_filled_2",
        F.substring("tag", 1, 2).alias("tag_prefix"),
        F.length("tag").alias("tag_length"),
        F.coalesce("tag", F.lit("OTHER")).alias("tag_filled_3"),
    )
)

base_carts = (
    filtered
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-02"))) & F.col("tag").isNotNull())
    .select("*", F.coalesce("tag", F.lit("Unknown")).alias("tag_filled"))
)

result = (
    base_carts
    .select(
        "created_at",
        "tag_lower",
        "tag_group",
        "tag_lower_2",
        "tag_filled_2",
        "tag_prefix",
        "tag_length",
        "tag_filled",
    )
)
