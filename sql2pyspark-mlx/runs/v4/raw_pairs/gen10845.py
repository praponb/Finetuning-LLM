from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.month(F.col("p.launch_date")).isin([8, 11, 12]))
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("p.supplier_id"),
        (F.col("ss.revenue") * F.col("ss.units")).alias("revenue_per_unit"),
    )
    .orderBy("id")
)
