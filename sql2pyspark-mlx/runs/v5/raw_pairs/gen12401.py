from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.avg(F.col("ss.units")).alias("avg_units"),
        (F.max(F.col("ss.revenue")) - F.min(F.col("ss.revenue"))).alias("revenue_range"),
    )
    .filter(F.col("revenue_range") >= 70)
)
