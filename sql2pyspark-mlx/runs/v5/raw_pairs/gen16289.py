from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "eco") | (F.col("p.stock_qty") < 339))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
    )
    .orderBy(F.col("stock_qty").desc())
)
