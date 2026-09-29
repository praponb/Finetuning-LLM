from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "order_id").orderBy(F.col("amount").asc_nulls_last())
w2 = Window.orderBy("amount", "id")
w3 = Window.partitionBy("status").orderBy("paid_at", "id")
w4 = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("seq_no").asc_nulls_first(), "id")

filtered = (
    df_payments
    .filter(F.col("method").isNotNull())
    .withColumn("method_title", F.initcap(F.trim("method")))
    .withColumn("amount_level", F.when(F.col("amount") >= 1540, "High").when(F.col("amount") >= 320, "Medium").otherwise("Low"))
)

enriched = (
    filtered
    .filter(F.col("paid_at") < "2025-04-01")
    .select(
        "id",
        "amount",
        "paid_at",
        "status",
        F.cume_dist().over(w).alias("amount_cume_dist"),
        F.lead("amount", 2).over(w2).alias("next_amount"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

with_prev = enriched.withColumn("prev_paid_at", F.lag("paid_at").over(w3))

gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("new_group_flag").over(w4))

latest = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "amount",
        "paid_at",
        "amount_cume_dist",
        "next_amount",
        "seq_no",
        "new_group_flag",
        "session_no",
    )
)

result = (
    latest
    .groupBy(
        F.when(F.col("amount") >= 1500, "HIGH").otherwise("LOW").alias("amount_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.min("session_no").alias("min_session_no"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)
