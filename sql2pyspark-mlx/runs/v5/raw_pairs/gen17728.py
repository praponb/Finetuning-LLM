from pyspark.sql import functions as F

quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

joined = (
    df_order_items
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.quantity",
        "src.unit_price",
        F.col("p.cost"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
    )
)

result = (
    joined
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "cost",
        "stock_qty",
        "product_name",
        F.when(F.col("product_name").rlike("^(wireless mouse|Yoga Mat)$"), "GROUP_A").when(F.col("product_name").rlike("^(Laptop Pro 14|USB-C Hub)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("product_name_zone"),
        F.col("avg_inventory_quantity"),
    )
    .orderBy(F.col("product_name_zone").desc())
)
