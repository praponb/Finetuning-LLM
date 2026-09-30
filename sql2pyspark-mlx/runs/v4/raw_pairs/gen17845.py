from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("tag"))

result = (
    exploded
    .select(
        "customer_id",
        "tag",
        F.when(F.col("tag").rlike("^(SPRING10|BUNDLE)$"), "EXTENDED").when(F.col("tag").rlike("^(VIP|WELCOME5)$"), "CORE").otherwise("PRIORITY").alias("tag_zone"),
    )
)
