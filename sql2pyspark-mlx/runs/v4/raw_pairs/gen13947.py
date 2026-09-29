from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

flattened = (
    lines
    .select("id", "created_at", "key", F.explode_outer("coupon_codes").alias("tag"))
)

valid = (
    flattened
    .filter(F.col("tag").isNotNull())
    .withColumn("tag_key", F.concat("tag", F.lit(" - "), "key"))
)

result = (
    valid
    .select(
        "key",
        "tag",
        F.when(F.col("tag").rlike("^(WELCOME5|VIP)$"), "EXTENDED").when(F.col("tag").rlike("^(SPRING10|BUNDLE)$"), "PRIORITY").otherwise("GROUP_B").alias("tag_zone"),
    )
    .orderBy(F.col("tag").desc())
)
