from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("customer_id").isin([17, 19, 25]))
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
        F.col("attributes")["gift"].alias("gift"),
    )
)
