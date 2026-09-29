from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .groupBy(F.col("s.id"), F.col("s.store_name"))
    .agg(
        F.count(F.col("ss.id")).alias("store_sale_count"),
        F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"),
    )
    .filter(F.col("store_sale_count") >= 1)
    .orderBy("avg_units", "id", "store_name")
    .limit(3)
)
