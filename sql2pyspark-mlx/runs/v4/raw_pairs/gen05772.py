from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("customer_id") == 23)
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.col("attributes")["device"].alias("device"),
    )
)
