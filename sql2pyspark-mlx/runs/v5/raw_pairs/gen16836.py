from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(
        F.date_format(F.col("p.launch_date"), "yyyy-MM").alias("launch_month"),
        F.quarter(F.col("p.launch_date")).alias("launch_quarter"),
    )
    .agg(
        F.count("*").alias("store_sale_count"),
        F.countDistinct(F.col("ss.units")).alias("unique_products"),
    )
)
