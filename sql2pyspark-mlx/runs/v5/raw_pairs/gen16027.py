from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.countDistinct("amount").alias("unique_channels"),
        F.countDistinct("discount").alias("unique_statuss"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .filter(F.col("t.unique_channels") > 5)
    .select(
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("t.unique_channels"),
        F.col("t.unique_statuss"),
    )
)
