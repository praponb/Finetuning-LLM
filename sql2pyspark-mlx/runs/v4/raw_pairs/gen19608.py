from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.channel").isin(["mobile", "store"]))
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("s.opened_date"),
    )
)
