from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_store_sales.alias("store_sales"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .filter(F.col("store_sales.store_id").isin([2, 6]) & (F.year(F.col("store_sales.sale_date")) >= 2024))
    .select(
        F.col("products.launch_date"),
        F.col("products.cost"),
        F.col("store_sales.revenue"),
        (F.col("store_sales.units") - F.col("store_sales.revenue")).alias("revenue_per_unit"),
    )
)
