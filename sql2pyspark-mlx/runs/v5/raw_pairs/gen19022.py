from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.city").alias("city"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("s.quantity")).alias("max_quantity"),
    )
    .orderBy("max_quantity")
)
