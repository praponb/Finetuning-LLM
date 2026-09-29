from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.col("ship_to.city").alias("ship_city"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.size("items").alias("num_items"),
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
    )
)
