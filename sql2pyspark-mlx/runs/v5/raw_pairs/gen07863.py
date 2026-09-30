from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("amount") < 700).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.order_id").isin([22, 30, 38]))
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("s.delivered_date"),
        F.col("s.order_id"),
    )
    .orderBy("delivered_date")
)
