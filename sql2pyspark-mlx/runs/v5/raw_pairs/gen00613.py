from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("island_id") >= 6, "LARGE").otherwise("SMALL").alias("island_id_tier"),
    )
    .agg(
        F.percentile_approx("island_id", 0.9).alias("island_id_p90"),
        F.min("paid_at").alias("first_seen"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
    )
    .orderBy("island_id_p90")
)
