from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_last(), "id")
w2 = Window.partitionBy("order_id")
w_part = Window.partitionBy("status", "order_id").orderBy("amount", "id")
w_recent = Window.orderBy("amount", "id")
w5 = Window.orderBy(F.col("prev_amount").asc_nulls_last())
w6 = Window.orderBy("order_id_sum_is_refunded", "id")

ranked = (
    df_payments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "order_id", "amount", "status")
)

cleaned = (
    ranked
    .filter(F.col("order_id").isNotNull() & (F.col("status") == "FAILED"))
    .withColumn("is_refunded", F.when(F.col("status") == "REFUNDED", 1).otherwise(0))
)

cleaned_payments = (
    cleaned
    .filter(F.col("status") == "FAILED")
    .select(
        "id",
        "order_id",
        "amount",
        "status",
        "is_refunded",
        F.when(F.col("amount") >= 1300, "High").when(F.col("amount") >= 300, "Medium").otherwise("Low").alias("amount_tier"),
    )
)

enriched = (
    cleaned_payments
    .select(
        "id",
        "amount",
        "is_refunded",
        "amount_tier",
        F.sum("is_refunded").over(w2).alias("order_id_sum_is_refunded"),
        F.lag("amount", 1, 0).over(w_part).alias("prev_amount"),
        F.lead("is_refunded", 1).over(w_recent).alias("next_is_refunded"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "amount",
        "is_refunded",
        "amount_tier",
        "order_id_sum_is_refunded",
        F.percent_rank().over(w5).alias("prev_amount_percent_rank"),
        F.lead("next_is_refunded", 1, 0).over(w6).alias("next_next_is_refunded"),
    )
)

summary = (
    with_windows
    .withColumn("is_refunded_tier", F.when(F.col("is_refunded") >= 79, "LARGE").otherwise("SMALL"))
    .withColumn("prev_amount_percent_rank_tier", F.when(F.col("prev_amount_percent_rank") >= 0.77, "LARGE").otherwise("SMALL"))
    .cube("is_refunded_tier", "prev_amount_percent_rank_tier")
    .agg(
        F.round(F.avg("is_refunded"), 2).alias("avg_is_refunded"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.count("*").alias("row_count"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.avg("order_id_sum_is_refunded").alias("avg_order_id_sum_is_refunded"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("row_count") > 1)
)

result = (
    summary
    .select(
        "is_refunded_tier",
        "prev_amount_percent_rank_tier",
        "avg_is_refunded",
        "amount_p50",
        "row_count",
        "amount_p95",
        "avg_order_id_sum_is_refunded",
    )
    .orderBy(F.col("avg_is_refunded").desc())
)
