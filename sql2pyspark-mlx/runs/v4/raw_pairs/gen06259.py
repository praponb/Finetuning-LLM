from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "attributes",
        F.col("ship_to.zip").alias("ship_zip"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.col("attributes").getItem("device").alias("device"),
        F.transform(
            F.filter("items", lambda item: item["promo"].isNotNull()),
            lambda item: F.struct(item["product_id"].alias("pid"), F.round(item["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)

result = (
    unpacked
    .groupBy("customer_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("created_at").alias("started_at"),
        F.max("created_at").alias("last_created_at"),
        F.countDistinct("device").alias("distinct_devices"),
        F.max("created_at").alias("last_seen"),
    )
    .filter(F.col("distinct_devices") < 1)
    .orderBy(F.col("distinct_devices").desc())
)
