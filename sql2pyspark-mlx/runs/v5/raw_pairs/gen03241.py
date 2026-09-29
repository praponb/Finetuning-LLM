from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.year(F.col("s.delivered_date")) == 2023)
    .groupBy(F.col("o.status"))
    .agg(F.min(F.col("s.delivered_date")).alias("first_delivered_date"))
    .orderBy("first_delivered_date")
)
