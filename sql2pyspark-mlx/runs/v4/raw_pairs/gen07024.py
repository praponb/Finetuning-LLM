from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.opened_date") <= "2025-04-27")
    .groupBy(F.quarter(F.col("p.launch_date")).alias("launch_quarter"))
    .agg(F.avg(F.col("ss.units")).alias("avg_units"))
    .filter(F.col("avg_units") >= 22)
    .orderBy(F.col("avg_units").desc())
)
