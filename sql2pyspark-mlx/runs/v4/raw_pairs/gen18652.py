from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("customer_id").isin([1, 10, 13, 29]))
    .select(
        "id",
        "customer_id",
        "ship_to",
        F.coalesce(F.col("attributes")["utm_source"], F.lit("direct")).alias("utm_source_or_default"),
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
    )
)
