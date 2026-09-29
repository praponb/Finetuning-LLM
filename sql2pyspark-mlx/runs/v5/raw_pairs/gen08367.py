from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("store_id") == 1).alias("i"), F.col("p.id") == F.col("i.product_id"), "left_anti")
    .filter(F.array_contains(F.col("p.tags"), "new"))
    .select(
        F.col("p.id"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("p.supplier_id"),
    )
)
