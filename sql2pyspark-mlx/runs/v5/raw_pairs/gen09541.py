from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("p.launch_date"),
    )
    .orderBy(F.col("revenue").desc(), "id")
)
