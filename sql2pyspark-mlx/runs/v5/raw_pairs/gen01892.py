from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "left")
    .groupingSets(
        [["channel", "order_status"], ["channel"], []],
        F.col("p.channel").alias("channel"),
        F.col("p.status").alias("order_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.cost")).alias("max_cost"),
    )
    .orderBy(F.col("max_cost").asc_nulls_last())
)
