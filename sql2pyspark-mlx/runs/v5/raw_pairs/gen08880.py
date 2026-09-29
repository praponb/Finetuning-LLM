from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") < "2023-12-12") | (F.col("quantity") >= 176))
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
    .orderBy(F.col("avg_quantity").desc(), "last_restock_month")
    .limit(5)
)
