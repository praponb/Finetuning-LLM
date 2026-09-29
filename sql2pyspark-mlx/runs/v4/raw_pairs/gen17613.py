from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w_recent = Window.partitionBy("carrier", "order_id").orderBy("cost", "id")
w3 = Window.orderBy(F.col("cost").desc_nulls_first())
w4 = Window.partitionBy("status").orderBy("cost_bucket_2", "id")
w5 = Window.partitionBy("order_id").orderBy("cost_bucket_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("cost_cume_dist", "id")
w7 = Window.partitionBy("carrier").orderBy(F.col("event_seq").desc())
w8 = Window.partitionBy("carrier", "status").orderBy("discount", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy("carrier").orderBy("order_date", "id")
w10 = Window.partitionBy("carrier")
w11 = Window.orderBy("event_seq", "id").rowsBetween(-6, Window.currentRow)
w12 = Window.partitionBy("status").orderBy("event_seq_cume_dist", "id").rowsBetween(-2, -1)
w13 = Window.partitionBy("status").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_shipments
    .select(
        "*",
        F.ntile(5).over(w_ordered).alias("cost_bucket"),
        F.ntile(10).over(w_ordered).alias("cost_bucket_2"),
        F.ntile(3).over(w_recent).alias("cost_bucket_3"),
    )
)

with_windows = (
    metrics
    .withColumn("cost_cume_dist", F.cume_dist().over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("last_known_cost_bucket", F.last("cost_bucket", ignorenulls=True).over(w5))
)

with_dims = (
    with_windows
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.order_date"), F.col("b.shipping_fee"))
)

metrics_shipments = (
    with_dims
    .withColumn("row_seq", F.row_number().over(w6))
    .withColumn("event_seq_cume_dist", F.cume_dist().over(w7))
)

joined = (
    metrics_shipments
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.amount"), F.col("b.discount"))
)

enriched = (
    joined
    .withColumn("event_seq_avg_3_prior", F.avg("event_seq").over(w8))
    .withColumn("next_event_seq_cume_dist", F.lead("event_seq_cume_dist", 1, 0).over(w9))
)

enriched_shipments = (
    enriched
    .select(
        "*",
        F.round((F.col("event_seq_avg_3_prior") / F.sum("event_seq_avg_3_prior").over(w10)), 4).alias("event_seq_avg_3_prior_share"),
        F.avg("discount").over(w11).alias("discount_avg_6"),
        F.avg("event_seq_cume_dist").over(w12).alias("event_seq_cume_dist_avg_2_prior"),
        F.first("next_event_seq_cume_dist", ignorenulls=True).over(w13).alias("first_known_next_event_seq_cume_dist"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w10)), 4).alias("event_seq_share"),
    )
)

result = (
    enriched_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "cost_bucket",
        "cost_bucket_3",
        "event_seq",
    )
)
