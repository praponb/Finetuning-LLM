from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "outer")
    .filter(F.col("ss.revenue") > 3900)
    .groupBy(F.col("p.brand"))
    .agg(
        F.max(F.col("ss.sale_date")).alias("latest_sale_date"),
        F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"),
    )
    .orderBy("latest_sale_date")
)
