from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("running_max_session_no").desc())
w5 = Window.orderBy(F.col("is_new_session").asc_nulls_first())
w6 = Window.orderBy(F.col("is_new_session").desc_nulls_first())
w7 = Window.partitionBy("txn_type")
w_part = Window.partitionBy("txn_type").orderBy(F.col("running_max_session_no_cume_dist").asc_nulls_first())
w9 = Window.partitionBy("merchant", "txn_type")

flagged = (
    df_transactions
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_session").over(w2))

enriched = (
    islands
    .select(
        "id",
        "txn_type",
        "merchant",
        "is_flagged",
        "is_new_session",
        F.sum("session_no").over(w3).alias("running_sum_session_no"),
        F.avg("session_no").over(w3).alias("running_avg_session_no"),
        F.max("session_no").over(w3).alias("running_max_session_no"),
    )
)

kept_keys = (
    enriched
    .select("txn_type", "merchant")
    .intersect(
        df_transactions
        .filter(~F.col("is_flagged"))
        .select("txn_type", "merchant")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.txn_type") == F.col("k.txn_type")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .withColumn("running_max_session_no_cume_dist", F.cume_dist().over(w4))
    .withColumn("is_new_session_cume_dist", F.cume_dist().over(w5))
)

enriched_transactions = (
    metrics
    .filter(F.col("is_new_session") > 1)
    .withColumn("is_new_session_cume_dist_2", F.cume_dist().over(w6))
    .withColumn("txn_type_stddev_samp_running_max_session_no", F.stddev_samp("running_max_session_no").over(w7))
)

metrics_transactions = (
    enriched_transactions
    .select(
        "id",
        "txn_type",
        "is_new_session_cume_dist",
        "is_new_session_cume_dist_2",
        "txn_type_stddev_samp_running_max_session_no",
        F.cume_dist().over(w_part).alias("running_max_session_no_cume_dist_cume_dist"),
        F.stddev_samp("txn_type_stddev_samp_running_max_session_no").over(w9).alias("merchant_stddev_samp_txn_type_stddev_samp_running_max_session_no"),
    )
)

valid = (
    metrics_transactions
    .filter(F.col("merchant_stddev_samp_txn_type_stddev_samp_running_max_session_no") < 6)
    .select(
        "id",
        "txn_type",
        "is_new_session_cume_dist",
        "txn_type_stddev_samp_running_max_session_no",
        "running_max_session_no_cume_dist_cume_dist",
        F.round("is_new_session_cume_dist", 2).alias("is_new_session_cume_dist_rounded"),
    )
)

result = (
    valid
    .groupBy("txn_type")
    .agg(
        F.round(F.sum("running_max_session_no_cume_dist_cume_dist") / F.nullif(F.count(F.when(F.col("txn_type") == "DEBIT", 1)), F.lit(0)), 2).alias("running_max_session_no_cume_dist_cume_dist_per_debit"),
        F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", F.col("running_max_session_no_cume_dist_cume_dist")).otherwise(0)).alias("credit_running_max_session_no_cume_dist_cume_dist"),
    )
)
