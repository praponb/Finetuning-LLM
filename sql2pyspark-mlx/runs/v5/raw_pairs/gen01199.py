from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("session_no").desc())
w5 = Window.partitionBy("account_id", "merchant")
w6 = Window.partitionBy("merchant").orderBy("session_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w8 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())
w9 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w10 = Window.partitionBy("account_id").orderBy("session_no_percent_rank", "id")
w_hist = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-6, Window.currentRow)
w12 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_ordered = Window.partitionBy("txn_type")
w14 = Window.partitionBy("account_id")

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("amount_level", F.when(F.col("amount") >= 3200, "High").when(F.col("amount") >= 1700, "Medium").otherwise("Low"))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)

with_gaps = (
    valid
    .withColumn("is_new_burst", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_no", F.sum("is_new_burst").over(w2))

scored = (
    sessions_transactions
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "amount_level",
        "is_flagged_flag",
        F.first("merchant", ignorenulls=True).over(w3).alias("first_known_merchant"),
        F.percent_rank().over(w4).alias("session_no_percent_rank"),
        F.max("is_new_burst").over(w5).alias("account_id_max_is_new_burst"),
        F.first("merchant", ignorenulls=True).over(w6).alias("first_known_merchant_2"),
    )
)

metrics = (
    scored
    .withColumn("amount_count_30d", F.count("amount").over(w7))
    .withColumn("amount_cume_dist", F.cume_dist().over(w8))
    .withColumn("account_id_max_is_new_burst_max_30d", F.max("account_id_max_is_new_burst").over(w9))
    .withColumn("cumulative_is_flagged_flag", F.sum("is_flagged_flag").over(w3))
    .withColumn("next_session_no_percent_rank", F.lead("session_no_percent_rank", 1).over(w10))
)

scored_transactions = (
    metrics
    .withColumn("last7_min_next_session_no_percent_rank", F.min("next_session_no_percent_rank").over(w_hist))
    .withColumn("last7_avg_next_session_no_percent_rank", F.avg("next_session_no_percent_rank").over(w_hist))
    .withColumn("last7_max_next_session_no_percent_rank", F.max("next_session_no_percent_rank").over(w_hist))
)

filtered = (
    scored_transactions
    .filter(F.col("merchant").isNotNull() & (~F.col("merchant").eqNullSafe(F.lit("Starbucks"))))
    .withColumn("session_no_percent_rank_per_amount_count_30d", F.round((F.col("session_no_percent_rank") / F.col("amount_count_30d")), 2))
)

scored_transactions_2 = (
    filtered
    .withColumn("account_id_max_is_new_burst_max_30d_max_7d", F.max("account_id_max_is_new_burst_max_30d").over(w12))
    .withColumn("txn_type_stddev_samp_last7_max_next_session_no_percent_rank", F.stddev_samp("last7_max_next_session_no_percent_rank").over(w_ordered))
    .withColumn("account_id_count_account_id_max_is_new_burst", F.count("account_id_max_is_new_burst").over(w14))
)

result = (
    scored_transactions_2
    .alias("s")
    .join(df_accounts.alias("dim"), F.col("s.account_id") == F.col("dim.id"), "left")
    .cube(F.col("dim.status").alias("status"), F.col("s.txn_type").alias("txn_type"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("s.account_id_max_is_new_burst_max_30d_max_7d")), 2).alias("avg_account_id_max_is_new_burst_max_30d_max_7d"),
        F.round(F.avg(F.col("s.last7_min_next_session_no_percent_rank")), 2).alias("avg_last7_min_next_session_no_percent_rank"),
    )
    .orderBy(F.col("txn_type").desc_nulls_first())
)
