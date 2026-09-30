from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("p.stock_qty") <= 303)
    .select(
        F.col("ss.revenue"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("s.region"),
        F.col("s.sqft"),
    )
)
