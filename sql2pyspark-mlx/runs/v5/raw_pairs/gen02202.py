from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("session_start_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("WEEK", "paid_at").alias("week_start"))
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("status") == "REFUNDED", 1)), F.lit(0)), 2).alias("session_id_per_refunded"),
        F.percentile_approx("session_id", [0.25, 0.5, 0.75]).alias("session_id_quartiles"),
    )
)
