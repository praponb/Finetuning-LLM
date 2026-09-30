from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("paid_at", "id")
w_recent = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("method").orderBy("amount", "id")
w6 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w_ordered = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status", "order_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("status").orderBy("paid_at", "id")
w10 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_payments
    .withColumn("amount_avg_3_prior", F.avg("amount").over(w_grp))
    .withColumn("amount_avg_30d", F.avg("amount").over(w2))
    .withColumn("prev_amount", F.lag("amount", 1).over(w3))
    .withColumn("running_amount", F.sum("amount").over(w_recent))
)

enriched = (
    with_windows
    .select(
        "*",
        F.row_number().over(w5).alias("seq_no"),
        F.sum("amount_avg_3_prior").over(w6).alias("amount_avg_3_prior_sum_90d"),
        F.last("amount", ignorenulls=True).over(w_ordered).alias("last_known_amount"),
        F.first("running_amount", ignorenulls=True).over(w8).alias("first_known_running_amount"),
    )
)

with_gaps = (
    enriched
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w9) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_payments = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w10))

valid = (
    sessions_payments
    .filter(F.col("method").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"),
        F.round("is_new_burst", 1).alias("is_new_burst_rounded"),
    )
)

result = (
    valid
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        "amount_avg_30d",
        "prev_amount",
        "running_amount",
        "is_new_burst",
        "session_no",
        F.when(F.col("status").rlike("^(FAILED|SUCCESS)$"), "CORE").when(F.col("status").rlike("^(REFUNDED)$"), "GROUP_A").otherwise("GROUP_B").alias("status_zone"),
    )
)
