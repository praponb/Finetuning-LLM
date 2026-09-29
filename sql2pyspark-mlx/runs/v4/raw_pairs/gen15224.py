from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("tag"))

result = (
    exploded
    .select(
        "tag",
        F.when(F.col("tag").rlike("^(VIP|WELCOME5)$"), "GROUP_B").otherwise("PRIORITY").alias("tag_zone"),
    )
    .orderBy(F.col("tag").desc())
)
