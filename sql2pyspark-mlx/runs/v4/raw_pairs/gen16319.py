from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id", "method").orderBy("paid_at", "id")
w5 = Window.partitionBy("status", "method").orderBy("burst_id", "id")
w6 = Window.partitionBy("method").orderBy("burst_id", "id")

valid = (
    df_payments
    .filter(F.hour("paid_at").between(9, 12))
    .select("*", F.initcap(F.trim("status")).alias("status_title"))
)

latest = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

flagged = (
    latest
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w2).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w2) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w3))

with_windows = (
    sessions_payments
    .select(
        "*",
        F.lag("amount", 2).over(w4).alias("prev_amount"),
        F.lead("new_group_flag", 1).over(w5).alias("next_new_group_flag"),
        F.ntile(4).over(w6).alias("burst_id_bucket"),
        F.lead("new_group_flag", 2).over(w2).alias("next_new_group_flag_2"),
    )
)

result = (
    with_windows
    .withColumn("prev_amount_tier", F.when(F.col("prev_amount") >= 1380, "LARGE").otherwise("SMALL"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "paid_at"))
    .rollup("prev_amount_tier", "quarter_start")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.percentile_approx("next_new_group_flag_2", [0.25, 0.5, 0.75]).alias("next_new_group_flag_2_quartiles"),
        F.grouping_id().alias("gid"),
    )
    .orderBy("prev_amount_tier")
)
