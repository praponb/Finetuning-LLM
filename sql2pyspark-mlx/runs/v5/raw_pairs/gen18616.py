from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "outer")
    .filter(F.col("ss.revenue") >= 2100)
    .select(
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
    )
)
