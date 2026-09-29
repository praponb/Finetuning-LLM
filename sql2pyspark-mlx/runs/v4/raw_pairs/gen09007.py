from pyspark.sql import functions as F

quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

joined = (
    df_order_items
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.launch_date"), F.col("p.brand"), F.col("p.price"))
)

result = (
    joined
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "launch_date",
        "brand",
        F.col("avg_inventory_quantity"),
    )
)
