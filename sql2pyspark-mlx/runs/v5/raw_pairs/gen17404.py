from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.qty"),
        F.col("line.promo"),
    )
)

exploded_carts = exploded.select("customer_id", "qty", F.explode_outer("coupon_codes").alias("tag"))

cleaned = (
    exploded_carts
    .filter(F.col("qty") >= 4)
    .select(
        "*",
        F.initcap(F.trim("tag")).alias("tag_title"),
        F.trim("tag").alias("clean_tag"),
        F.coalesce("tag", F.lit("OTHER")).alias("resolved_tag"),
    )
)

valid = (
    cleaned
    .filter(F.col("tag").isNotNull())
    .select(
        "customer_id",
        "qty",
        "tag",
        "tag_title",
        "clean_tag",
        "resolved_tag",
        F.coalesce("tag", F.lit("N/A")).alias("tag_filled"),
        F.coalesce("tag", F.lit("N/A")).alias("tag_filled_2"),
    )
)

result = valid.select("customer_id", "tag_title", "resolved_tag")
