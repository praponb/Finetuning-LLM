from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id")
w2 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter((F.col("method") == "bank_transfer") & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.length("status").alias("status_length"),
    )
)

with_gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .withColumn("burst_id_tier", F.when(F.col("burst_id") >= 7, "LARGE").otherwise("SMALL"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 1770, "HIGH").otherwise("LOW"))
    .rollup("burst_id_tier", "amount_tier", "burst_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("paid_at").alias("last_paid_at"),
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.countDistinct("status").alias("distinct_status"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("burst_id").desc())
)
