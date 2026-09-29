from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w))

flagged = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .withColumn("new_group_flag_tier", F.when(F.col("new_group_flag") >= 1, "LARGE").otherwise("SMALL"))
    .cube("new_group_flag_tier", "status")
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("status") == "FAILED", 1)), F.lit(0)), 2).alias("amount_per_failed"),
        F.sum("new_group_flag").alias("total_new_group_flag"),
        F.max("paid_at").alias("ended_at"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("total_new_group_flag").desc_nulls_first())
)
