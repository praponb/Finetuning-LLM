from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id_tier").orderBy(F.col("distinct_ids").desc())

cleaned = (
    df_payments
    .filter(F.col("amount").between(810, 970) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.length("status").alias("status_length"),
    )
)

with_gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_payments = (
    with_gaps
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

result = (
    sessions_payments
    .groupBy(
        F.when(F.col("session_id") >= 8, "LARGE").otherwise("SMALL").alias("session_id_tier"),
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .withColumn("distinct_ids_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("activity_month").asc_nulls_first())
)
