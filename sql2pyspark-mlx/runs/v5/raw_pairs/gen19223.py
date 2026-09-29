from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.product_name").like("S%"))
    .select(
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.col("p.cost"),
        F.col("p.launch_date"),
    )
)
