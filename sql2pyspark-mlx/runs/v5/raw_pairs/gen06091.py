from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["city", "region"], ["city"]],
        F.col("p.city").alias("city"),
        F.col("p.region").alias("region"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.quantity")).alias("min_quantity"),
        F.grouping_id().alias("grouping_level"),
    )
)
