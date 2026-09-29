from pyspark.sql import functions as F

exploded = df_carts.select("id", F.explode("coupon_codes").alias("tag"))

result = (
    exploded
    .select(
        "tag",
        F.when(F.col("tag").rlike("^(VIP|BUNDLE)$"), "OTHER_ZONE").when(F.col("tag").rlike("^(SPRING10|WELCOME5)$"), "GROUP_B").otherwise("PRIORITY").alias("tag_zone"),
    )
    .orderBy(F.col("tag").asc_nulls_last())
)
