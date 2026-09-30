from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("created_at") < "2024-02-01")
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "attributes",
        F.size("items").alias("num_items"),
        F.forall("items", lambda i: i["qty"] <= 5).alias("all_small_qty"),
    )
)
