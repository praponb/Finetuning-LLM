from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2024-04-07 10:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.concat_ws("-", "status", "method").alias("status_method_key"),
    )
)

flagged = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("QUARTER", "paid_at").alias("quarter_start"))
    .agg(
        F.percentile_approx("new_group_flag", [0.25, 0.5, 0.75]).alias("new_group_flag_quartiles"),
        F.stddev_samp("amount").alias("amount_stddev"),
    )
)
