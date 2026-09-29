from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

result = (
    flattened
    .select(
        "id",
        "customer_id",
        "created_at",
        "tag",
        F.when(F.col("tag").rlike("^(BUNDLE|VIP)$"), "PRIORITY").when(F.col("tag").rlike("^(SPRING10|WELCOME5)$"), "OTHER_ZONE").otherwise("CORE").alias("tag_zone"),
    )
)
