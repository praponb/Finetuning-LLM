from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "ship_to",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.col("attributes")["device"].alias("device"),
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)

result = (
    flattened_attrs
    .cube("customer_id")
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
)
