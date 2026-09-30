from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "right")
    .filter(F.col("o.order_date") < "2024-05-01")
    .groupBy(F.quarter(F.col("o.order_date")).alias("order_quarter"))
    .agg(
        F.count(F.col("s.cost")).alias("cost_count"),
        F.count("*").alias("cnt"),
    )
    .orderBy("order_quarter")
    .limit(1)
)
