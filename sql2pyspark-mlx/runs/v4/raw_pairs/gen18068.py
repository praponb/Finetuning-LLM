from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

lines = exploded.select("k", "v", F.explode("coupon_codes").alias("tag"))

valid = (
    lines
    .filter(F.col("tag") == "BUNDLE")
    .withColumn("tag_length", F.length("tag"))
    .withColumn("k_tag", F.concat("k", F.lit(", "), "tag"))
)

result = valid.select("v", "tag", "k_tag")
