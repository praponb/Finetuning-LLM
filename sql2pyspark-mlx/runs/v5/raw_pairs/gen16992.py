from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status", "method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id")
w3 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("method").orderBy("is_new_session", "id")
w5 = Window.partitionBy("method").orderBy(F.col("is_new_session").desc(), "id")
w6 = Window.partitionBy("order_id", "method").orderBy(F.col("prev_is_new_session").desc_nulls_last(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.avg("cost").alias("avg_shipment_cost"))

enriched = (
    df_payments
    .withColumn("prev_amount", F.lag("amount").over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w_ordered))
)

with_gaps = (
    enriched
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w2) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w3))

scored = (
    sessions_payments
    .select(
        "id",
        "order_id",
        "method",
        "seq_no",
        "is_new_session",
        F.lead("is_new_session").over(w4).alias("next_is_new_session"),
        F.lag("is_new_session").over(w4).alias("prev_is_new_session"),
        F.lag("seq_no").over(w4).alias("prev_seq_no"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "is_new_session",
        "next_is_new_session",
        "prev_is_new_session",
        "prev_seq_no",
        "dedup_rank",
        "rn",
        F.col("avg_shipment_cost"),
    )
    .orderBy(F.col("is_new_session").desc())
)
