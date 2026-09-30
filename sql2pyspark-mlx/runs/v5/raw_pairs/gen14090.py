from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "status").orderBy("paid_at_period", "id")
w2 = Window.partitionBy("order_id", "status")
w3 = Window.partitionBy("status").orderBy("paid_at_period_2", "id")
w4 = Window.partitionBy("status").orderBy("paid_at_period_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_payments
    .filter((F.col("status") == "REFUNDED") & (F.col("paid_at") > F.lit("2023-10-25 22:00:00").cast("timestamp")))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.date_trunc("MONTH", "paid_at").alias("paid_at_period"),
        F.date_trunc("year", "paid_at").alias("paid_at_period_2"),
    )
)

metrics = (
    cleaned
    .filter(F.col("paid_at_period_2") >= "2025-02-01")
    .select(
        "id",
        "order_id",
        "status",
        "paid_at_period_2",
        F.lead("amount").over(w).alias("next_amount"),
        F.row_number().over(w).alias("row_seq"),
        F.sum("amount").over(w2).alias("order_id_sum_amount"),
    )
)

gaps = (
    metrics
    .withColumn("is_new_burst", F.when(F.col("paid_at_period_2") - F.lag("paid_at_period_2").over(w3) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "status",
        "paid_at_period_2",
        "next_amount",
        F.sum("is_new_burst").over(w4).alias("island_id"),
    )
)

gaps_2 = (
    islands
    .withColumn("is_new_session", F.when(F.lag("paid_at_period_2").over(w3).isNull() | (F.col("paid_at_period_2") > F.lag("paid_at_period_2").over(w3) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps_2.withColumn("island_id_2", F.sum("is_new_session").over(w4))

result = (
    sessions_payments
    .select(
        "id",
        "status",
        "paid_at_period_2",
        "next_amount",
        "island_id",
        "is_new_session",
        "island_id_2",
        F.when(F.col("status").rlike("^(SUCCESS|FAILED)$"), "GROUP_A").when(F.col("status").rlike("^(REFUNDED)$"), "PRIORITY").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
