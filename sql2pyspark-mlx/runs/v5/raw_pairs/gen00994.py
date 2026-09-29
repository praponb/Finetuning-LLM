from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("txn_type").orderBy(F.col("last3_sum_burst_id").desc_nulls_last(), "id")
w_running = Window.partitionBy("txn_type").orderBy("last3_avg_burst_id", "id")
w6 = Window.partitionBy("txn_type").orderBy("last3_avg_burst_id", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy("txn_type").orderBy("last3_avg_burst_id_avg_2", "id")
w8 = Window.partitionBy("txn_type").orderBy("burst_id")
w9 = Window.partitionBy("txn_type").orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("txn_type").orderBy(F.col("burst_id").desc(), "id")

with_dims = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.amount",
        "a.txn_time",
        F.col("d.opened_date"),
    )
)

with_gaps = (
    with_dims
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "opened_date",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

with_windows = (
    islands
    .filter(F.col("burst_id").between(4, 5))
    .select(
        "id",
        "txn_type",
        "burst_id",
        F.min("burst_id").over(w_part).alias("last3_min_burst_id"),
        F.max("burst_id").over(w_part).alias("last3_max_burst_id"),
        F.sum("burst_id").over(w_part).alias("last3_sum_burst_id"),
        F.avg("burst_id").over(w_part).alias("last3_avg_burst_id"),
    )
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("id", "txn_type", "burst_id", "last3_avg_burst_id")
)

metrics = (
    latest
    .select(
        "id",
        "txn_type",
        "burst_id",
        "last3_avg_burst_id",
        F.lead("last3_avg_burst_id").over(w_running).alias("next_last3_avg_burst_id"),
        F.lead("burst_id").over(w_running).alias("next_burst_id"),
        F.row_number().over(w_running).alias("seq_no"),
        F.avg("last3_avg_burst_id").over(w6).alias("last3_avg_burst_id_avg_2"),
    )
)

with_windows_transactions = (
    metrics
    .withColumn("prev_last3_avg_burst_id", F.lag("last3_avg_burst_id", 1).over(w7))
    .withColumn("burst_id_cume_dist", F.cume_dist().over(w8))
)

scored = (
    with_windows_transactions
    .select(
        "id",
        "burst_id",
        "last3_avg_burst_id",
        "next_burst_id",
        "prev_last3_avg_burst_id",
        "burst_id_cume_dist",
        F.avg("prev_last3_avg_burst_id").over(w9).alias("running_avg_prev_last3_avg_burst_id"),
        F.min("prev_last3_avg_burst_id").over(w9).alias("running_min_prev_last3_avg_burst_id"),
        F.count("prev_last3_avg_burst_id").over(w9).alias("running_count_prev_last3_avg_burst_id"),
        F.sum("prev_last3_avg_burst_id").over(w9).alias("running_sum_prev_last3_avg_burst_id"),
        F.ntile(10).over(w10).alias("burst_id_bucket"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("burst_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("running_sum_prev_last3_avg_burst_id", 0.5).alias("running_sum_prev_last3_avg_burst_id_p50"),
        F.percentile_approx("burst_id", [0.25, 0.5, 0.75]).alias("burst_id_quartiles"),
        F.sum("running_avg_prev_last3_avg_burst_id").alias("total_running_avg_prev_last3_avg_burst_id"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy("distinct_ids")
)
