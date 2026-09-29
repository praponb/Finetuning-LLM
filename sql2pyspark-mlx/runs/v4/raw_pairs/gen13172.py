from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.store_id").isin([5, 6]))
    .groupBy(F.col("s.city"))
    .agg(
        F.max(F.col("ss.units")).alias("max_units"),
        F.avg(F.col("ss.units")).alias("avg_units"),
    )
    .filter(F.col("avg_units") > 15)
    .orderBy("city")
)
