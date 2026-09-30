from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

with_latest = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.status").alias("shipment_status"),
        F.col("c.delivered_date").alias("shipment_delivered_date"),
    )
)

result = (
    with_latest
    .groupBy(
        "shipment_status",
        F.date_trunc("WEEK", "shipment_delivered_date").alias("week_start"),
    )
    .agg(
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.size(F.collect_set("shipment_status")).alias("num_shipment_status"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sort_array(F.collect_set("shipment_status")).alias("shipment_status_list"),
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "CANCELLED", 1)), F.lit(0)), 2).alias("amount_per_cancelled"),
    )
    .orderBy(F.col("week_start").asc_nulls_first())
)
