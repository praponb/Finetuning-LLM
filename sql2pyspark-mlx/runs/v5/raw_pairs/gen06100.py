from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attributes",
        F.col("attributes").getItem("device").alias("device"),
        F.size("items").alias("num_items"),
        F.size("attributes").alias("num_attributes"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
    )
)

result = (
    flattened_attrs
    .groupBy("customer_id")
    .agg(
        F.max("num_items").alias("max_num_items"),
        F.percentile_approx("num_items", 0.95).alias("num_items_p95"),
    )
    .orderBy(F.col("customer_id").desc_nulls_last())
)
