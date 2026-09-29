from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("method").orderBy("paid_at", "id")
w4 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter(F.col("paid_at") >= "2023-04-01")
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.round((F.col("amount") * 100), 2).alias("adjusted_amount"),
        F.upper("method").alias("method_upper"),
        F.coalesce("method", F.lit("DIRECT")).alias("method_clean"),
    )
)

gaps = (
    base
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

gaps_2 = (
    islands
    .withColumn("new_group_flag_2", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_payments = gaps_2.withColumn("session_id", F.sum("new_group_flag_2").over(w2))

with_prev = sessions_payments.withColumn("prev_paid_at", F.lag("paid_at").over(w3))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments_2 = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "adjusted_amount",
        "method_upper",
        "session_id",
        F.sum("is_new_session").over(w4).alias("island_id_2"),
    )
)

result = (
    sessions_payments_2
    .groupBy(F.date_trunc("year", "paid_at").alias("year_start"))
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.count_if(F.col("method") == "paypal").alias("paypal_events"),
        F.sum(F.when(F.col("method") == "card", F.col("adjusted_amount")).otherwise(0)).alias("card_adjusted_amount"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_rows"),
    )
    .orderBy("duration_min")
)
