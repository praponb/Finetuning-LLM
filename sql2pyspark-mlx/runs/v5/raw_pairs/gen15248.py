from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .groupBy(
        F.col("o.status").alias("order_status"),
        F.col("e.carrier").alias("carrier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("e.cost")).alias("avg_cost"),
    )
    .orderBy(F.col("avg_cost").desc_nulls_first())
)
