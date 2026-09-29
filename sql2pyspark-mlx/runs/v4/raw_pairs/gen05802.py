from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("tag"))

filtered = (
    lines
    .filter(F.col("customer_id").isNotNull())
    .select("*", F.coalesce("tag", F.lit("N/A")).alias("tag_filled"))
)

valid = (
    filtered
    .filter(F.col("tag") != "VIP")
    .select(
        "*",
        F.trim("tag").alias("clean_tag"),
        F.initcap(F.trim("tag")).alias("tag_title"),
        F.coalesce("tag", F.lit("NONE")).alias("resolved_tag"),
    )
)

valid_carts = (
    valid
    .filter(F.col("customer_id").isNotNull())
    .select("customer_id", "tag", "clean_tag", F.length("tag").alias("tag_length"))
)

result = (
    valid_carts
    .groupBy("customer_id")
    .pivot("tag", ["SPRING10", "BUNDLE", "VIP"])
    .agg(F.avg("tag_length"))
)
