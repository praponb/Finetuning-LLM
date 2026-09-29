from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("amount") >= 1640, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.countDistinct("method").alias("distinct_methods"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
    .orderBy(F.col("failed_count").asc_nulls_first())
)
