from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount_scaled").asc_nulls_first())
w2 = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("order_id")
w_hist = Window.partitionBy("method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w7 = Window.partitionBy("status", "order_id")
w8 = Window.partitionBy("status", "method").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

filtered = (
    df_payments
    .filter(F.col("paid_at") >= F.lit("2023-05-16 09:00:00").cast("timestamp"))
    .withColumn("amount_scaled", F.round((F.col("amount") * 1.15), 2))
    .withColumn("method_status_key", F.concat_ws("-", "method", "status"))
)

joined = (
    filtered
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.discount"))
)

remaining_keys = (
    joined
    .select("order_id", "id")
    .exceptAll(
        df_payments
        .filter(F.col("order_id").isin([3, 9, 21, 37]))
        .select("order_id", "id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.dense_rank().over(w).alias("amount_scaled_dense_rank"),
        F.avg("discount").over(w2).alias("discount_avg_30d"),
        F.count("amount").over(w3).alias("amount_count_7d"),
    )
)

metrics = (
    scored
    .withColumn("status_sum_amount_scaled_dense_rank", F.sum("amount_scaled_dense_rank").over(w4))
    .withColumn("amount_scaled_dense_rank_share", F.round((F.col("amount_scaled_dense_rank") / F.sum("amount_scaled_dense_rank").over(w5)), 4))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w4)), 4))
    .withColumn("amount_count_7d_avg_90d", F.avg("amount_count_7d").over(w_hist))
    .withColumn("status_sum_amount_scaled", F.sum("amount_scaled").over(w7))
)

enriched = (
    metrics
    .select(
        "*",
        F.sum("amount").over(w7).alias("status_sum_amount"),
        F.avg("status_sum_amount_scaled_dense_rank").over(w8).alias("status_sum_amount_scaled_dense_rank_avg_7d"),
    )
)

valid = (
    enriched
    .filter((F.col("paid_at") >= F.lit("2024-01-08 23:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "method_status_key",
        "discount",
        "amount_scaled_dense_rank",
        "discount_avg_30d",
        "amount_count_7d",
        "amount_share",
        "status_sum_amount_scaled",
        "status_sum_amount",
        "status_sum_amount_scaled_dense_rank_avg_7d",
        F.col("status_sum_amount_scaled").cast("string").alias("status_sum_amount_scaled_num"),
        F.when(F.col("status_sum_amount") >= 6270, "High").when(F.col("status_sum_amount") >= 2876, "Medium").otherwise("Low").alias("status_sum_amount_band"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("method_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("amount") >= 1330, "LARGE").otherwise("SMALL").alias("amount_tier"),
        "status",
        F.when(F.col("status_sum_amount") >= 6413, "PREMIUM").otherwise("STANDARD").alias("status_sum_amount_tier"),
    )
    .agg(
        F.round(F.avg("status_sum_amount_scaled_dense_rank_avg_7d"), 2).alias("avg_status_sum_amount_scaled_dense_rank_avg_7d"),
        F.min("paid_at").alias("started_at"),
        F.min("paid_at").alias("started_at_2"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("status_sum_amount")).otherwise(0)).alias("failed_status_sum_amount"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
    )
    .orderBy("status")
)
