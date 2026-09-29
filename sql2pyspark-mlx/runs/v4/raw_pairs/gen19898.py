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
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.size("items").alias("num_items"),
        F.col("ship_to.zip").alias("ship_zip"),
    )
)
