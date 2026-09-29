from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.category"), F.col("p.supplier_id"))
    .agg(F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"))
    .orderBy("supplier_id", "category")
    .limit(10)
)
