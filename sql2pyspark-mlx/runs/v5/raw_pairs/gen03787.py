from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        F.transform("items", lambda x: x["product_id"]).alias("product_ids"),
        F.coalesce(F.col("attributes")["device"], F.lit("unknown")).alias("device_or_default"),
        F.size("items").alias("num_items"),
        F.array_contains(F.map_keys("attributes"), "gift").alias("has_gift"),
    )
)
