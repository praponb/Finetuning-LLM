from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "ship_to",
        "attributes",
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
        F.array_contains(F.map_keys("attributes"), "utm_source").alias("has_utm_source"),
    )
)
