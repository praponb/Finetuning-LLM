from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.round(F.avg(F.col("p.stock_qty")), 2).alias("avg_stock_qty"),
    )
)
