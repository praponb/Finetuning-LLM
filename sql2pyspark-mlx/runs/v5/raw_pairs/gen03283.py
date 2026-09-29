from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.city").alias("city"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("s.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("city").desc())
)
