from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.month(F.col("ss.sale_date")).isin([2, 7, 9]) | (F.col("p.stock_qty") > 152))
    .select(
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
    )
)
