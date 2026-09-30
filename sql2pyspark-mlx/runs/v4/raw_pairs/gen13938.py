from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .filter(F.col("p.price") <= 300)
    .groupBy(F.col("p.supplier_id"))
    .agg(
        F.count(F.col("ss.revenue")).alias("revenue_count"),
        F.avg(F.col("ss.units")).alias("avg_units"),
    )
)
