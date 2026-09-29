from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "created_at", "coupon_codes", F.explode("attributes").alias("k", "v"))
)

exploded_carts = exploded.select("id", "k", "v", F.explode_outer("coupon_codes").alias("tag"))

cleaned = (
    exploded_carts
    .filter(F.col("tag") == "BUNDLE")
    .select("*", F.trim("tag").alias("clean_tag"))
)

cleaned_carts = (
    cleaned
    .filter(F.col("tag").isNotNull())
    .select(
        "id",
        "k",
        "v",
        "tag",
        "clean_tag",
        F.when(F.col("k").isin(["utm_source", "weight_kg"]), "Group A").otherwise("Other").alias("k_group"),
        F.coalesce("tag", F.lit("UNKNOWN")).alias("tag_filled"),
    )
)

result = cleaned_carts.select("k", "v", "tag", "clean_tag", "k_group", "tag_filled")
