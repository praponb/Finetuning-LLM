from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id", "status")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("order_id", "status").orderBy(F.col("order_id_stddev_samp_cost").desc())
w4 = Window.partitionBy("carrier", "order_status")
w_part = Window.partitionBy("order_status", "status")

remaining_keys = (
    df_shipments
    .select("carrier")
    .exceptAll(
        df_shipments
        .filter(F.col("cost") >= 56)
        .select("carrier")
    )
)

reconciled = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .withColumn("order_id_stddev_samp_cost", F.stddev_samp("cost").over(w_grp))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w2)), 4))
)

enriched_shipments = (
    enriched
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount") > 610)
    .select("a.*", F.col("o.channel"), F.col("o.status").alias("order_status"))
)

enriched_shipments_2 = (
    enriched_shipments
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.order_date"), F.col("p.discount"))
)

metrics = (
    enriched_shipments_2
    .select(
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost_share",
        "order_date",
        F.percent_rank().over(w3).alias("order_id_stddev_samp_cost_percent_rank"),
        F.round((F.col("cost") / F.sum("cost").over(w4)), 4).alias("cost_share_2"),
        F.count("discount").over(w_part).alias("order_status_count_discount"),
    )
)

result = (
    metrics
    .select(
        "shipped_date",
        "delivered_date",
        "cost_share",
        "order_id_stddev_samp_cost_percent_rank",
        "cost_share_2",
    )
    .orderBy("cost_share_2")
)
