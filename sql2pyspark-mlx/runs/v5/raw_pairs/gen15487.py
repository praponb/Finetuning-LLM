from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_payments
    .withColumn("is_new_burst", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

result = (
    sessionized
    .groupBy(F.date_trunc("year", "paid_at").alias("year_start"))
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("method") == "bank_transfer", 1)), F.lit(0)), 2).alias("amount_per_bank_transfer"),
        F.percentile_approx("session_id", 0.5).alias("session_id_p50"),
        F.size(F.collect_set("method")).alias("num_methods"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("distinct_ids")
)
