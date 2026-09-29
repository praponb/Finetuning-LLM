from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "ship_to",
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
    )
)

result = (
    parsed
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("event_count").asc_nulls_first())
)
