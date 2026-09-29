from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_payments
    .filter((F.col("paid_at") > F.to_timestamp(F.lit("2023-07-26 07:00:00"))) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.substring("method", 1, 3).alias("method_prefix"),
        F.when(F.col("method").isin(["cash", "promptpay"]), "Priority").otherwise("Other").alias("method_group"),
    )
)

flagged = (
    valid
    .withColumn("is_new_burst", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("activity_month"))
    .agg(
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("is_new_burst")).otherwise(0)).alias("bank_transfer_is_new_burst"),
    )
)
