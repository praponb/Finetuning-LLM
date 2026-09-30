from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "LARGE").otherwise("SMALL").alias("session_start_flag_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.size(F.collect_set("method")).alias("num_methods"),
    )
    .orderBy(F.col("session_start_flag_tier").asc_nulls_last())
)
