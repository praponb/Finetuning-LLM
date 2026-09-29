from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id")
w2 = Window.partitionBy("method").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("amount").desc())
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("status", "order_id").orderBy(F.col("paid_at").desc(), "id")
w6 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w7 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.to_timestamp(F.lit("2023-06-03 14:00:00"))) & (~F.col("method").eqNullSafe(F.lit("cash"))))
    .select("*", F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"))
)

cleaned = (
    filtered
    .filter(F.col("method").isNotNull())
    .select(
        "*",
        F.lower("method").alias("method_lower"),
        F.lower("method").alias("method_lower_2"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.lead("amount", 2, 0).over(w).alias("next_amount"),
        F.first("method", ignorenulls=True).over(w2).alias("first_known_method"),
        F.rank().over(w3).alias("amount_rank"),
        F.min("amount").over(w4).alias("status_min_amount"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

with_prev = enriched.withColumn("prev_paid_at", F.lag("paid_at").over(w6))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_paid_at").isNull() | (F.col("paid_at") > F.col("prev_paid_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = flagged.withColumn("burst_id", F.sum("is_new_session").over(w7))

result = (
    sessions_payments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .groupingSets(
        [["order_status", "method"], ["order_status"], []],
        F.col("o.status").alias("order_status"),
        F.col("src.method").alias("method"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("src.amount_rank")).alias("total_amount_rank"),
        F.grouping_id().alias("level_id"),
    )
)
