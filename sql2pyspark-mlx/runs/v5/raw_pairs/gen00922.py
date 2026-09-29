from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("island_id", "id")
w_grp = Window.partitionBy("order_id", "status").orderBy("new_group_flag", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.orderBy(F.col("prev_island_id").desc())
w6 = Window.orderBy("island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("new_group_flag", "id")
w8 = Window.orderBy("prev_island_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w11 = Window.orderBy("new_group_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("amount_avg_5", "id")

gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

metrics = (
    sessionized
    .select(
        "id",
        "paid_at",
        "new_group_flag",
        "island_id",
        F.lag("island_id", 1).over(w3).alias("prev_island_id"),
        F.avg("amount").over(w_grp).alias("amount_avg_5"),
    )
)

metrics_payments = (
    metrics
    .filter(F.hour("paid_at").between(10, 16))
    .withColumn("prev_island_id_percent_rank", F.percent_rank().over(w5))
    .withColumn("cumulative_amount_avg_5", F.sum("amount_avg_5").over(w6))
)

metrics_payments_2 = (
    metrics_payments
    .select(
        "id",
        "paid_at",
        "new_group_flag",
        "island_id",
        "amount_avg_5",
        "prev_island_id_percent_rank",
        "cumulative_amount_avg_5",
        F.lag("prev_island_id").over(w7).alias("prev_prev_island_id"),
        F.lead("island_id").over(w7).alias("next_island_id"),
        F.row_number().over(w7).alias("row_seq"),
        F.last("prev_island_id_percent_rank", ignorenulls=True).over(w8).alias("last_known_prev_island_id_percent_rank"),
    )
)

enriched = (
    metrics_payments_2
    .select(
        "*",
        F.sum("row_seq").over(w9).alias("cumulative_row_seq"),
        F.sum("prev_island_id_percent_rank").over(w_hist).alias("prev_island_id_percent_rank_sum_90d"),
        F.last("amount_avg_5", ignorenulls=True).over(w11).alias("last_known_amount_avg_5"),
        F.lead("island_id", 2).over(w_recent).alias("next_island_id_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "paid_at",
        "new_group_flag",
        "prev_island_id_percent_rank",
        "cumulative_amount_avg_5",
        "prev_prev_island_id",
        "next_island_id",
        "row_seq",
        "cumulative_row_seq",
    )
    .orderBy(
        F.col("prev_prev_island_id").desc_nulls_first(),
        "prev_island_id_percent_rank",
    )
)
