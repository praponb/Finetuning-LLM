from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        F.array_contains(F.map_keys("attributes"), "utm_source").alias("has_utm_source"),
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
    )
)

result = (
    parsed
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.count("*").alias("event_count_2"),
    )
)
