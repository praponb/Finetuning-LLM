from pyspark.sql import functions as F

lines = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("tag"))

result = (
    lines
    .select(
        "created_at",
        "tag",
        F.when(F.col("tag").rlike("^(VIP|WELCOME5)$"), "GROUP_B").when(F.col("tag").rlike("^(SPRING10|BUNDLE)$"), "EXTENDED").otherwise("PRIORITY").alias("tag_zone"),
    )
    .orderBy(F.col("tag").desc(), "created_at")
)
