from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "items",
        F.array_contains(F.map_keys("attributes"), "utm_source").alias("has_utm_source"),
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
    )
)
