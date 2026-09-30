from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("paid_at", "id")
w2 = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-06-14 09:00:00"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.when(F.col("amount") >= 1400, "High").when(F.col("amount") >= 520, "Medium").otherwise("Low").alias("amount_level"),
        F.when(F.col("amount") >= 1340, "High").when(F.col("amount") >= 600, "Medium").otherwise("Low").alias("amount_tier"),
    )
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessionized
    .withColumn("amount_tier", F.when(F.col("amount") >= 1530, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["amount_tier", "burst_id"], ["amount_tier"], []],
        "amount_tier",
        "burst_id",
    )
    .agg(
        F.max("paid_at").alias("ended_at"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.grouping("amount_tier").alias("grouping_id"),
    )
    .filter(F.col("avg_amount") > 0)
)
