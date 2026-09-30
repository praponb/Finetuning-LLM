from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("carrier")
w3 = Window.orderBy(F.col("cost").desc_nulls_last())
w4 = Window.partitionBy("carrier", "order_id").orderBy(F.col("cost").asc_nulls_last())
w5 = Window.partitionBy("order_id", "status").orderBy(F.col("order_date").desc_nulls_last(), "id")

ranked = (
    df_shipments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "status",
        F.stddev_samp("cost").over(w2).alias("carrier_stddev_samp_cost"),
        F.percent_rank().over(w3).alias("cost_percent_rank"),
        F.rank().over(w4).alias("cost_rank"),
    )
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.shipping_fee"),
        F.col("d.order_date"),
        F.col("d.status").alias("order_status"),
    )
)

latest = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .groupingSets(
        [["order_status", "channel", "shipment_order_status"], ["order_status"], []],
        F.col("o.status").alias("order_status"),
        F.col("o.channel").alias("channel"),
        F.col("e.order_status").alias("shipment_order_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("e.carrier_stddev_samp_cost")).alias("min_carrier_stddev_samp_cost"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("channel").desc_nulls_first())
)
