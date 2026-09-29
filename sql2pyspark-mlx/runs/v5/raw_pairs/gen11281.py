from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("week_start").orderBy(F.col("num_methods").desc())

cleaned = (
    df_payments
    .filter((~F.col("method").eqNullSafe(F.lit("bank_transfer"))) & (F.col("paid_at") >= F.lit("2023-05-15").cast("date")))
    .select("*", F.datediff(F.current_date(), "paid_at").alias("days_since_paid_at"))
)

flagged = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(
        F.date_trunc("WEEK", "paid_at").alias("week_start"),
        F.date_trunc("QUARTER", "paid_at").alias("quarter_start"),
    )
    .agg(
        F.size(F.collect_set("method")).alias("num_methods"),
        F.min("paid_at").alias("started_at"),
    )
    .withColumn("num_methods_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("quarter_start").desc())
)
