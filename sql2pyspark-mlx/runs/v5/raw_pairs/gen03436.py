from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.year("order_date") >= 2024).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("s.carrier"),
        F.col("s.delivered_date"),
    )
)
