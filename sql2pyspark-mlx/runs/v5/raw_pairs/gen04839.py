from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("is_card", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("method", "status").orderBy(F.col("is_card").desc())
w3 = Window.partitionBy("order_id").orderBy("paid_at", "id")
w4 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy(F.col("last4_min_is_card").desc_nulls_last(), "id")

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-08-30 06:00:00").cast("timestamp")) & (F.col("method") == "bank_transfer"))
    .withColumn("is_card", F.when(F.col("method") == "card", 1).otherwise(0))
    .withColumn("clean_method", F.trim("method"))
)

metrics = (
    base
    .withColumn("last4_max_is_card", F.max("is_card").over(w))
    .withColumn("last4_sum_is_card", F.sum("is_card").over(w))
    .withColumn("last4_count_is_card", F.count("is_card").over(w))
    .withColumn("last4_min_is_card", F.min("is_card").over(w))
    .withColumn("is_card_dense_rank", F.dense_rank().over(w2))
)

flagged = (
    metrics
    .withColumn("new_group_flag", F.when(F.col("paid_at") - F.lag("paid_at").over(w3) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "clean_method",
        "last4_max_is_card",
        "last4_count_is_card",
        "last4_min_is_card",
        "is_card_dense_rank",
        F.sum("new_group_flag").over(w4).alias("session_id"),
    )
)

deduped = (
    sessionized
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    deduped
    .groupBy("order_id")
    .pivot("status", ["FAILED", "SUCCESS"])
    .agg(
        F.max("last4_count_is_card").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "order_id",
        F.col("FAILED_value_sum").alias("failed_value_sum"),
        F.col("FAILED_item_count").alias("failed_item_count"),
        F.col("SUCCESS_value_sum").alias("success_value_sum"),
        F.col("SUCCESS_item_count").alias("success_item_count"),
    )
)
