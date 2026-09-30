from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.revenue") > 3000)
    .groupBy(F.col("p.category"))
    .agg(
        F.countDistinct(F.col("ss.revenue")).alias("unique_stores"),
        F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"),
    )
    .filter(F.col("unique_stores") > 2)
)
