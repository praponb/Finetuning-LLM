from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .withColumn("week_start", F.date_trunc("week", "paid_at"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 1330, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["week_start", "amount_tier", "session_id"], ["week_start", "amount_tier"], ["week_start"], []],
        "week_start",
        "amount_tier",
        "session_id",
    )
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.countDistinct("method").alias("distinct_methods"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("session_start_flag")).otherwise(0)).alias("refunded_session_start_flag"),
    )
    .orderBy(F.col("week_start").desc())
)
