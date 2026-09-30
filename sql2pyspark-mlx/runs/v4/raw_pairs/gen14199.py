from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("store_id") == 24).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.delivered_date") <= "2023-04-01")
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.status"),
        F.col("s.carrier"),
    )
    .orderBy("carrier")
)
