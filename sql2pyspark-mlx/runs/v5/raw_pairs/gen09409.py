from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("carrier").orderBy(F.col("cost").asc_nulls_first())
w_ordered = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(-3, Window.currentRow)
w_ordered_2 = Window.orderBy("delivered_date", "id")
w_recent = Window.partitionBy("carrier").orderBy("cost_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("delivered_date", "id")
w8 = Window.partitionBy("status").orderBy(F.col("running_min_seq_no").desc(), "id")

with_windows = (
    df_shipments
    .withColumn("order_id_max_cost", F.max("cost").over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("cost_percent_rank", F.percent_rank().over(w3))
    .withColumn("cost_avg_3", F.avg("cost").over(w_ordered))
    .withColumn("next_cost", F.lead("cost", 1).over(w_ordered_2))
)

scored = (
    with_windows
    .withColumn("running_avg_seq_no", F.avg("seq_no").over(w_recent))
    .withColumn("running_sum_seq_no", F.sum("seq_no").over(w_recent))
    .withColumn("running_max_seq_no", F.max("seq_no").over(w_recent))
    .withColumn("running_min_seq_no", F.min("seq_no").over(w_recent))
)

metrics = (
    scored
    .withColumn("prev_seq_no", F.lag("seq_no").over(w7))
    .withColumn("next_seq_no", F.lead("seq_no").over(w7))
    .withColumn("next_cost_avg_3", F.lead("cost_avg_3").over(w7))
    .withColumn("seq_no_2", F.row_number().over(w7))
    .withColumn("running_min_seq_no_bucket", F.ntile(5).over(w8))
)

remaining_keys = (
    metrics
    .select("order_id", "status")
    .exceptAll(
        df_shipments
        .filter(F.col("order_id").isNull())
        .select("order_id", "status")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id").eqNullSafe(F.col("b.id")), "inner")
    .select("e.*", F.col("b.discount"))
)

enriched = (
    joined
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.shipping_fee").between(15, 26))
    .select(
        "src.id",
        "src.order_id",
        "src.carrier",
        "src.shipped_date",
        "src.delivered_date",
        "src.cost",
        "src.order_id_max_cost",
        "src.cost_avg_3",
        "src.next_cost",
        "src.running_avg_seq_no",
        "src.running_sum_seq_no",
        "src.next_seq_no",
        "src.running_min_seq_no_bucket",
        "src.discount",
        F.col("o.amount"),
        F.col("o.status").alias("order_status"),
    )
)

result = (
    enriched
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["status", "order_status"], ["status"], []],
        F.col("d.status").alias("status"),
        F.col("src.order_status").alias("order_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("src.order_id_max_cost")).alias("max_order_id_max_cost"),
        F.max(F.col("src.cost_avg_3")).alias("max_cost_avg_3"),
        F.grouping_id().alias("grouping_level"),
    )
)
