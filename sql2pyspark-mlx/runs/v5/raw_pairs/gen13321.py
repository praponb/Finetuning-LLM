from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attributes",
        F.transform(
            F.filter("items", lambda li: li["promo"].isNotNull()),
            lambda li: F.struct(li["product_id"].alias("pid"), F.round(li["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.transform("items", lambda li: li["product_id"]).alias("product_ids"),
    )
)

result = (
    unpacked
    .rollup("customer_id")
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("event_count").desc())
)
