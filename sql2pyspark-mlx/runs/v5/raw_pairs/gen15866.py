from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.product_name").like("%Hub%"))
    .select(
        F.col("i.last_restock"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("p.brand"),
    )
)
