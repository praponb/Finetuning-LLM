from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .filter(F.upper(F.trim(F.col("p.product_name"))) == "LAPTOP PRO 14")
    .groupBy(F.month(F.col("p.launch_date")).alias("launch_month"), F.col("p.brand"))
    .agg(
        F.min(F.col("ss.units")).alias("min_units"),
        F.avg(F.col("ss.units")).alias("avg_units"),
    )
    .orderBy(F.col("brand").desc(), "launch_month")
    .limit(1)
)
