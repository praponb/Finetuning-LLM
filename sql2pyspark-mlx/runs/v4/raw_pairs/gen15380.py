from pyspark.sql import functions as F

cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("cost").alias("avg_product_cost"))

eligible = (
    df_products
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.quantity") <= 3), "left_anti")
)

result = (
    eligible
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "price",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.when(F.col("product_name").rlike("^(Laptop Pro 14|Running Shoes)$"), "PRIORITY").otherwise("GROUP_A").alias("product_name_zone"),
        F.col("avg_product_cost"),
    )
)
