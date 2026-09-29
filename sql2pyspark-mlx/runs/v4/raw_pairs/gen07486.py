from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("session_id") >= 8, "LARGE").otherwise("SMALL").alias("session_id_tier"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("session_start_flag")).otherwise(0)).alias("refunded_session_start_flag"),
        F.max("paid_at").alias("ended_at"),
    )
    .filter(F.col("refunded_session_start_flag") >= 0)
)
