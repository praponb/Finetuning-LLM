from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"))
    .agg(
        F.count("*").alias("total_count"),
        F.avg(F.col("ss.units")).alias("avg_units"),
    )
    .orderBy(F.col("avg_units").desc())
)
