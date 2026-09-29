from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.shipped_date").between("2023-10-01", "2024-09-01") | (F.col("s.order_id") == 35))
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("s.carrier"),
        F.col("s.shipped_date"),
        F.col("o.amount"),
        F.concat(F.col("o.channel"), F.lit(" - "), F.col("o.status")).alias("channel_status"),
    )
)
