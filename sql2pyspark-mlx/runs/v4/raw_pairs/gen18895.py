from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.category").isin(["Sports", "Grocery"]) & (F.col("p.cost") <= 370))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.revenue"),
        F.col("p.stock_qty"),
        F.col("s.opened_date"),
    )
    .orderBy("opened_date", "revenue", "id")
    .limit(50)
)
