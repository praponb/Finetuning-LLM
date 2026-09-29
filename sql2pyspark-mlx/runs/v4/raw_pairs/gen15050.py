from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_payments
    .withColumn("new_group_flag_tier", F.when(F.col("new_group_flag") >= 1, "LARGE").otherwise("SMALL"))
    .rollup("order_id", "new_group_flag_tier")
    .agg(
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_rows"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count_2"),
        F.sum(F.when(F.col("method") == "card", F.col("island_id")).otherwise(0)).alias("card_island_id"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("failed_count_2") > 1)
)
