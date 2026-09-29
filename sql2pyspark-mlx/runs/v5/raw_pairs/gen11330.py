from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "outer")
    .filter(F.col("p.supplier_id") == 39)
    .groupBy(F.col("p.brand"))
    .agg(
        F.min(F.col("ss.sale_date")).alias("min_sale_date"),
        F.avg(F.col("ss.units")).alias("avg_units"),
    )
    .orderBy("brand", "avg_units")
)
