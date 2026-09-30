from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_first())
w3 = Window.partitionBy("status", "method").orderBy("row_num", "id")
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("order_id").orderBy(F.col("row_num").desc(), "id")
w6 = Window.partitionBy("method", "order_id").orderBy("amount_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("order_id").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w9 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("method", "status").orderBy("amount", "id")
w11 = Window.partitionBy("order_id", "method").orderBy(F.col("event_seq").asc_nulls_last())

deduped = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .withColumn("amount_cume_dist", F.cume_dist().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w4)), 4))
)

scored = (
    enriched
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("first_known_method", F.first("method", ignorenulls=True).over(w6))
    .withColumn("cumulative_amount_cume_dist", F.sum("amount_cume_dist").over(w_running))
)

metrics = (
    scored
    .filter(F.col("event_seq") < 5)
    .withColumn("row_seq_sum_30d", F.sum("row_seq").over(w8))
    .withColumn("last_known_amount_share", F.last("amount_share", ignorenulls=True).over(w9))
    .withColumn("next_amount_share", F.lead("amount_share", 1, 0).over(w10))
    .withColumn("event_seq_cume_dist", F.cume_dist().over(w11))
    .withColumn("event_seq_share", F.round((F.col("event_seq") / F.sum("event_seq").over(w4)), 4))
)

remaining_keys = (
    metrics
    .select("method", "order_id")
    .intersectAll(
        df_payments
        .filter(F.col("status") != "FAILED")
        .select("method", "order_id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupingSets([["method", "order_id"], ["method"], []], "method", "order_id")
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
        F.percentile_approx("event_seq_share", [0.25, 0.5, 0.75]).alias("event_seq_share_quartiles"),
        F.max("event_seq").alias("max_event_seq"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("refunded_amount") > 0)
)
