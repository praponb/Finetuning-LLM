from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "attributes",
        F.coalesce(F.col("attributes").getItem("device"), F.lit("direct")).alias("device_or_default"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("direct")).alias("utm_source_or_default"),
        F.size("attributes").alias("num_attributes"),
    )
)
