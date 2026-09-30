from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.hour("created_at").between(11, 19))
    .select(
        "id",
        "created_at",
        "items",
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.col("ship_to.city").alias("ship_city"),
    )
)
