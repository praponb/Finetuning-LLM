from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.city").isin(["New York", "Tokyo"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(
        F.count(F.col("ss.revenue")).alias("revenue_count"),
        F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"),
    )
    .filter(F.col("revenue_count") > 3)
)
