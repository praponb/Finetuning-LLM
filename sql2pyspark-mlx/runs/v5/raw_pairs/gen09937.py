from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount_whole").desc())
w2 = Window.partitionBy("status")
w3 = Window.orderBy("amount")
w_hist = Window.orderBy(F.col("status_count_amount_share").asc_nulls_last())
w_part = Window.orderBy(F.col("amount_share_times_status_count_amount_share").asc_nulls_first())

filtered = (
    df_payments
    .filter(F.col("amount") >= 1210)
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.ceil("amount").alias("amount_whole"),
        F.coalesce("method", F.lit("OTHER")).alias("resolved_method"),
    )
)

matched_keys = (
    filtered
    .select("order_id", "method")
    .intersectAll(
        df_payments
        .filter(F.col("order_id") == 6)
        .select("order_id", "method")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "id",
        "status",
        "resolved_method",
        F.dense_rank().over(w).alias("amount_whole_dense_rank"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.cume_dist().over(w3).alias("amount_cume_dist"),
    )
)

with_windows_payments = (
    with_windows
    .select(
        "id",
        "amount_share",
        F.count("amount_share").over(w2).alias("status_count_amount_share"),
    )
)

cleaned = (
    with_windows_payments
    .filter(F.col("amount_share") < 0.33)
    .select(
        "*",
        (F.col("amount_share") * F.col("status_count_amount_share")).alias("amount_share_times_status_count_amount_share"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "status_count_amount_share",
        "amount_share_times_status_count_amount_share",
        F.percent_rank().over(w_hist).alias("status_count_amount_share_percent_rank"),
        F.cume_dist().over(w_part).alias("amount_share_times_status_count_amount_share_cume_dist"),
    )
)

result = (
    enriched
    .withColumn("status_count_amount_share_tier", F.when(F.col("status_count_amount_share") >= 17, "LARGE").otherwise("SMALL"))
    .withColumn("amount_share_times_status_count_amount_share_tier", F.when(F.col("amount_share_times_status_count_amount_share") >= 75, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["status_count_amount_share_tier", "amount_share_times_status_count_amount_share_tier"], ["status_count_amount_share_tier"], []],
        "status_count_amount_share_tier",
        "amount_share_times_status_count_amount_share_tier",
    )
    .agg(
        F.avg("amount_share_times_status_count_amount_share").alias("avg_amount_share_times_status_count_amount_share"),
        F.sum("amount_share_times_status_count_amount_share_cume_dist").alias("total_amount_share_times_status_count_amount_share_cume_dist"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("status_count_amount_share_tier").alias("gid"),
    )
)
