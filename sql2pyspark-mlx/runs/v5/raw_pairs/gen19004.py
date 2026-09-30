from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method", "order_id").orderBy("paid_at", "id")
w4 = Window.partitionBy("order_id", "method")
w5 = Window.partitionBy("status").orderBy("next_new_group_flag", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("method").orderBy(F.col("next_new_group_flag").desc_nulls_last())
w7 = Window.partitionBy("method", "status").orderBy("next_new_group_flag", "id")
w8 = Window.partitionBy("status").orderBy(F.col("order_id_avg_island_id_avg_5").desc_nulls_last(), "id")
w9 = Window.partitionBy("status").orderBy(F.col("next_new_group_flag_percent_rank").desc(), "id")

with_prev = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

enriched = (
    sessionized
    .select(
        "id",
        "method",
        "status",
        "prev_paid_at",
        "new_group_flag",
        F.lead("new_group_flag", 1).over(w3).alias("next_new_group_flag"),
        F.avg("island_id").over(w4).alias("order_id_avg_island_id"),
    )
)

with_windows = (
    enriched
    .filter(F.col("order_id_avg_island_id") == 6)
    .select(
        "id",
        "status",
        "prev_paid_at",
        "next_new_group_flag",
        "order_id_avg_island_id",
        F.avg("order_id_avg_island_id").over(w5).alias("order_id_avg_island_id_avg_5"),
        F.percent_rank().over(w6).alias("next_new_group_flag_percent_rank"),
        F.lead("next_new_group_flag", 1).over(w7).alias("next_next_new_group_flag"),
    )
)

ranked = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w8))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "status",
        "prev_paid_at",
        "order_id_avg_island_id_avg_5",
        "next_new_group_flag_percent_rank",
        "next_next_new_group_flag",
    )
)

deduped = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select(
        "id",
        "status",
        "next_new_group_flag_percent_rank",
        "next_next_new_group_flag",
        "rank_in_group",
    )
    .orderBy(F.col("rank_in_group").desc_nulls_first())
)
