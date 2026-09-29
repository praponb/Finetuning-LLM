from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("method")
w5 = Window.partitionBy("status", "method")
w6 = Window.partitionBy("method").orderBy(F.col("status_sum_amount_max_30d").asc_nulls_first())

matched_keys = (
    df_payments
    .select("method", "status")
    .intersectAll(
        df_payments
        .filter(F.col("status") != "REFUNDED")
        .select("method", "status")
    )
)

reconciled = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.method") == F.col("k.method")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "id",
        "method",
        "amount",
        "status",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.sum("amount").over(w2).alias("amount_sum_30d"),
        F.max("amount").over(w3).alias("amount_max_30d"),
    )
)

enriched = (
    with_windows
    .withColumn("amount_share_2", F.round((F.col("amount") / F.sum("amount").over(w4)), 4))
    .withColumn("status_avg_amount_sum_30d", F.avg("amount_sum_30d").over(w))
)

metrics = (
    enriched
    .select(
        "id",
        "method",
        F.avg("amount_share").over(w5).alias("status_avg_amount_share"),
        F.sum("amount_max_30d").over(w).alias("status_sum_amount_max_30d"),
    )
)

enriched_payments = (
    metrics
    .select(
        "id",
        "method",
        "status_avg_amount_share",
        F.round((F.col("status_sum_amount_max_30d") / F.sum("status_sum_amount_max_30d").over(w4)), 4).alias("status_sum_amount_max_30d_share"),
        F.cume_dist().over(w6).alias("status_sum_amount_max_30d_cume_dist"),
    )
)

result = (
    enriched_payments
    .withColumn("status_sum_amount_max_30d_share_tier", F.when(F.col("status_sum_amount_max_30d_share") >= 0.9, "LARGE").otherwise("SMALL"))
    .rollup("status_sum_amount_max_30d_share_tier")
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.countDistinct("method").alias("distinct_methods"),
        F.percentile_approx("status_sum_amount_max_30d_cume_dist", [0.25, 0.5, 0.75]).alias("status_sum_amount_max_30d_cume_dist_quartiles"),
        F.percentile_approx("status_avg_amount_share", [0.25, 0.5, 0.75]).alias("status_avg_amount_share_quartiles"),
        F.sum(F.when(F.col("method") == "cash", F.col("status_sum_amount_max_30d_share")).otherwise(0)).alias("cash_status_sum_amount_max_30d_share"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("cash_status_sum_amount_max_30d_share") > 0)
)
