from pyspark.sql import functions as F

by_dimension = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["order_status", "channel", "carrier"], ["order_status"], []],
        F.col("d.status").alias("order_status"),
        F.col("d.channel").alias("channel"),
        F.col("src.carrier").alias("carrier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("src.cost")), 2).alias("avg_cost"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    by_dimension
    .select("cnt", "avg_cost")
    .orderBy(F.col("cnt").asc_nulls_first())
)
