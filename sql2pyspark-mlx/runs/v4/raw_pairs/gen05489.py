from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-12-05 19:00:00").cast("timestamp")) & F.col("order_id").isNotNull() & F.col("amount").between(1180, 1430))
    .withColumn("status_prefix", F.substring("status", 1, 3))
)

with_gaps = (
    valid
    .withColumn("new_group_flag", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

cleaned = (
    islands
    .filter(F.col("amount") > 1530)
    .select(
        "id",
        "method",
        "amount",
        "status",
        "status_prefix",
        "new_group_flag",
        "burst_id",
        F.date_trunc("year", "paid_at").alias("paid_at_period"),
    )
)

stats = (
    cleaned
    .groupBy("method")
    .agg(
        F.max("paid_at_period").alias("ended_at"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
    )
)

result = (
    stats
    .select(
        "method",
        F.when(F.col("method").rlike("^(promptpay|paypal)$"), "OTHER_ZONE").when(F.col("method").rlike("^(card|bank_transfer)$"), "GROUP_A").otherwise("EXTENDED").alias("method_zone"),
    )
    .orderBy(F.col("method_zone").asc_nulls_first(), "method")
)
