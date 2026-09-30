from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "outer")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .groupBy(
        F.when(F.col("products.stock_qty") >= 433, "high").when(F.col("products.stock_qty") >= 69, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.min(F.col("inventory.quantity")).alias("min_quantity"),
        F.sum(F.col("inventory.quantity")).alias("total_quantity"),
    )
    .filter(F.col("min_quantity") >= 100)
    .orderBy("stock_qty_bucket")
    .limit(50)
)
