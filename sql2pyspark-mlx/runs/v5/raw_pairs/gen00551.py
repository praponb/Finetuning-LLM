from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("method") == "card") & (F.col("method").eqNullSafe(F.lit("paypal"))))
    .withColumn("days_since_paid_at", F.datediff(F.current_date(), "paid_at"))
)

gaps = (
    base
    .withColumn("is_new_session", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        "days_since_paid_at",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .groupingSets(
        [["session_no", "year_start"], ["session_no"]],
        "session_no",
        "year_start",
    )
    .agg(
        F.percentile_approx("session_no", 0.5).alias("session_no_p50"),
        F.size(F.collect_set("method")).alias("num_methods"),
    )
)
