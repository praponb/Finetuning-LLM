from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount_rounded", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("paid_at", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("method").orderBy("paid_at", "id")
w5 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("paid_at", "id")
w7 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED"]))
    .withColumn("amount_rounded", F.round("amount", 2))
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.sum("amount_rounded").over(w2).alias("running_sum_amount_rounded"),
        F.avg("amount_rounded").over(w2).alias("running_avg_amount_rounded"),
        F.count("amount_rounded").over(w2).alias("running_count_amount_rounded"),
        F.avg("amount").over(w3).alias("amount_avg_5"),
    )
)

with_gaps = (
    with_windows
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w4) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w5))

with_gaps_2 = (
    islands
    .withColumn("new_group_flag_2", F.when(F.lag("paid_at").over(w4).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w4) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps_2.withColumn("burst_id", F.sum("new_group_flag_2").over(w5))

with_gaps_3 = (
    sessionized
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w6) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized_payments = with_gaps_3.withColumn("burst_id_2", F.sum("session_start_flag").over(w7))

result = (
    sessionized_payments
    .select(
        "id",
        "status",
        F.col("running_avg_amount_rounded").alias("RUNNING_AVG_AMOUNT_ROUNDED"),
        F.col("running_sum_amount_rounded").alias("RUNNING_SUM_AMOUNT_ROUNDED_VAL"),
        F.col("amount").alias("AMOUNT"),
    )
    .unpivot(
        ids=["id", "status"],
        values=["RUNNING_AVG_AMOUNT_ROUNDED", "RUNNING_SUM_AMOUNT_ROUNDED_VAL", "AMOUNT"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
)
