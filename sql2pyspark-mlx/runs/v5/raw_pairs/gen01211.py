from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .cube(F.col("p.region").alias("region"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("s.quantity")).alias("avg_quantity"),
        F.grouping_id().alias("gid"),
    )
)
