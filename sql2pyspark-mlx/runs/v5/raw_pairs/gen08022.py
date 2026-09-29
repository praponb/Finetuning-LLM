from pyspark.sql import functions as F

stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("stock_qty").alias("max_product_stock_qty"))

eligible = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.product_id") == 9), "left_anti")
)

result = (
    eligible
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.when(F.col("brand").rlike("^(Stark|Umbrella)$"), "EXTENDED").otherwise("PRIORITY").alias("brand_zone"),
        F.col("max_product_stock_qty"),
    )
)
