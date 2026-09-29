from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy(F.col("shipped_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("order_id").orderBy("rn", "id")
w3 = Window.partitionBy("channel").orderBy(F.col("discount").asc_nulls_last(), "id")
w4 = Window.partitionBy("channel").orderBy("rank_in_group", "id")
w5 = Window.partitionBy("channel").orderBy(F.col("cost").desc())

deduped = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_dims = (
    deduped
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.customer_id").isNull())
    .select(
        "s.id",
        "s.order_id",
        "s.carrier",
        "s.delivered_date",
        "s.cost",
        "s.rn",
        F.col("b.channel"),
        F.col("b.status").alias("order_status"),
        F.col("b.discount"),
    )
)

latest = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

deduped_shipments = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    deduped_shipments
    .select(
        "*",
        F.lag("discount").over(w4).alias("prev_discount"),
        F.lag("cost").over(w4).alias("prev_cost"),
        F.lead("cost").over(w4).alias("next_cost"),
        F.row_number().over(w4).alias("event_seq"),
        F.percent_rank().over(w5).alias("cost_percent_rank"),
    )
)

result = (
    enriched
    .alias("e")
    .join(df_orders.alias("dim"), F.col("e.order_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["status", "order_channel"], ["status"]],
        F.col("dim.status").alias("status"),
        F.col("dim.channel").alias("order_channel"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("e.event_seq")).alias("avg_event_seq"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("status").desc())
)
