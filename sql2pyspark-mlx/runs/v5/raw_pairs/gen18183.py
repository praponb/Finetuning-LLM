from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_payments = (
    with_gaps
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.sum("new_group_flag").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_payments
    .rollup(
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
        "order_id",
        F.date_trunc("YEAR", "paid_at").alias("year_start"),
    )
    .agg(
        F.round(F.sum("burst_id") / F.nullif(F.count(F.when(F.col("status") == "REFUNDED", 1)), F.lit(0)), 2).alias("burst_id_per_refunded"),
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        F.count("*").alias("num_rows"),
    )
)
