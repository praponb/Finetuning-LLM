from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .groupBy(
        F.col("p.brand"),
        F.date_format(F.col("p.launch_date"), "yyyy-MM").alias("launch_month"),
    )
    .agg(F.count(F.col("ss.revenue")).alias("revenue_count"))
)
