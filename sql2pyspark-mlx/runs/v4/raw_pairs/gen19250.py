from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        "ship_to",
        "attributes",
        F.size("items").alias("num_items"),
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.transform(
            F.filter("items", lambda li: li["promo"].isNotNull()),
            lambda li: F.struct(li["product_id"].alias("pid"), F.round(li["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
    )
)

result = (
    flattened_attrs
    .rollup("customer_id")
    .agg(
        F.percentile_approx("num_items", 0.9).alias("num_items_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("num_items").alias("min_num_items"),
        F.grouping("customer_id").alias("grouping_level"),
    )
    .filter(F.col("num_items_p90") >= 0)
    .orderBy(F.col("customer_id").desc_nulls_first())
)
