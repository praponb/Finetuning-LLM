from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.filter(F.col("region") == "Central").alias("s"), F.col("o.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("o.discount") > 0.27)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("o.store_id"),
    )
)
