from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_payments
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .groupingSets([["year_start", "status"], ["year_start"]], "year_start", "status")
    .agg(
        F.min("paid_at").alias("first_seen"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.min("paid_at").alias("first_seen_2"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
    )
)
