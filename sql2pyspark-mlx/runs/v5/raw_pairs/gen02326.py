from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("account_id", "merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("txn_type").orderBy("amount", "id")
w5 = Window.partitionBy("account_id").orderBy("running_sum_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("txn_type", "account_id").orderBy("prev_running_sum_amount", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("merchant")
w8 = Window.partitionBy("txn_type", "merchant").orderBy(F.col("last4_count_prev_amount").desc_nulls_first())
w9 = Window.partitionBy("txn_type", "account_id").orderBy(F.col("burst_id").desc())
w10 = Window.partitionBy("merchant").orderBy("running_max_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_transactions
    .filter(F.col("amount").between(1500, 3900))
    .withColumn("running_max_amount", F.max("amount").over(w_running))
    .withColumn("running_count_amount", F.count("amount").over(w_running))
    .withColumn("running_sum_amount", F.sum("amount").over(w_running))
)

gaps = (
    with_windows
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w2).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w2) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_session").over(w3))

enriched = (
    islands
    .select(
        "*",
        F.lag("running_sum_amount").over(w4).alias("prev_running_sum_amount"),
        F.lag("amount").over(w4).alias("prev_amount"),
        F.lead("amount").over(w4).alias("next_amount"),
        F.row_number().over(w4).alias("row_seq"),
        F.first("running_max_amount", ignorenulls=True).over(w5).alias("first_known_running_max_amount"),
    )
)

metrics = (
    enriched
    .withColumn("last4_avg_prev_amount", F.avg("prev_amount").over(w_hist))
    .withColumn("last4_min_prev_amount", F.min("prev_amount").over(w_hist))
    .withColumn("last4_count_prev_amount", F.count("prev_amount").over(w_hist))
    .withColumn("merchant_avg_running_sum_amount", F.avg("running_sum_amount").over(w7))
)

metrics_transactions = (
    metrics
    .withColumn("last4_count_prev_amount_rank", F.rank().over(w8))
    .withColumn("running_sum_amount_share", F.round((F.col("running_sum_amount") / F.sum("running_sum_amount").over(w7)), 4))
    .withColumn("burst_id_cume_dist", F.cume_dist().over(w9))
    .withColumn("last_known_prev_amount", F.last("prev_amount", ignorenulls=True).over(w10))
)

result = (
    metrics_transactions
    .withColumn("burst_id_cume_dist_tier", F.when(F.col("burst_id_cume_dist") >= 0.84, "PREMIUM").otherwise("STANDARD"))
    .withColumn("year_start", F.date_trunc("year", "txn_time"))
    .cube("burst_id_cume_dist_tier", "year_start", "account_id")
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.count("*").alias("num_rows"),
        F.max("txn_time").alias("last_seen"),
        F.sum(F.when(F.col("merchant") == "Shell", F.col("last4_count_prev_amount_rank")).otherwise(0)).alias("shell_last4_count_prev_amount_rank"),
        F.count_if(F.col("txn_type") == "REFUND").alias("refund_events"),
        F.round(F.sum("running_count_amount") / F.nullif(F.count(F.when(F.col("merchant") == "Shell", 1)), F.lit(0)), 2).alias("running_count_amount_per_shell"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("span_minutes") < 1000000)
    .orderBy(F.col("running_count_amount_per_shell").desc_nulls_last())
)
