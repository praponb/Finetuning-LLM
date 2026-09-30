from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount")
w_recent = Window.partitionBy("txn_type", "account_id").orderBy("txn_time", "id")
w3 = Window.orderBy("txn_time", "id").rowsBetween(-2, -1)
w_grp = Window.partitionBy("merchant").orderBy("amount", "id")
w5 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w6 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("account_id").orderBy("amount_cume_dist", "id")
w8 = Window.partitionBy("account_id", "merchant").orderBy("txn_time", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy("account_id")
w10 = Window.partitionBy("account_id").orderBy("prev_amount_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("account_id", "txn_type").orderBy(F.col("prev_amount_2").asc_nulls_last())
w12 = Window.partitionBy("account_id").orderBy(F.col("is_new_session").desc())
w13 = Window.partitionBy("merchant", "account_id")
w14 = Window.orderBy("prev_amount_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w15 = Window.partitionBy("merchant").orderBy(F.col("island_id").desc())
w16 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w17 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w18 = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc(), "id")

enriched = (
    df_transactions
    .withColumn("amount_cume_dist", F.cume_dist().over(w))
    .withColumn("prev_amount", F.lag("amount", 2).over(w_recent))
    .withColumn("amount_avg_2_prior", F.avg("amount").over(w3))
    .withColumn("prev_amount_2", F.lag("amount", 2).over(w_grp))
)

flagged = (
    enriched
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w5).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w5) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_session").over(w6))

with_windows = (
    sessionized
    .select(
        "*",
        F.lead("island_id", 2).over(w7).alias("next_island_id"),
        F.avg("amount_avg_2_prior").over(w8).alias("amount_avg_2_prior_avg_3_prior"),
        F.max("is_new_session").over(w9).alias("account_id_max_is_new_session"),
        F.last("amount", ignorenulls=True).over(w10).alias("last_known_amount"),
        F.dense_rank().over(w11).alias("prev_amount_2_dense_rank"),
    )
)

metrics = (
    with_windows
    .withColumn("is_new_session_cume_dist", F.cume_dist().over(w12))
    .withColumn("prev_amount_2_share", F.round((F.col("prev_amount_2") / F.sum("prev_amount_2").over(w13)), 4))
    .withColumn("first_known_prev_amount_2_dense_rank", F.first("prev_amount_2_dense_rank", ignorenulls=True).over(w14))
)

with_windows_transactions = (
    metrics
    .select(
        "*",
        F.rank().over(w15).alias("island_id_rank"),
        F.count("is_new_session_cume_dist").over(w16).alias("is_new_session_cume_dist_count_90d"),
        F.max("next_island_id").over(w17).alias("next_island_id_max_7d"),
        F.row_number().over(w18).alias("event_seq"),
    )
)

result = (
    with_windows_transactions
    .groupBy(
        F.date_trunc("WEEK", "txn_time").alias("week_start"),
        F.when(F.col("prev_amount_2_share") >= 0.87, "PREMIUM").otherwise("STANDARD").alias("prev_amount_2_share_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.percentile_approx("prev_amount", 0.95).alias("prev_amount_p95"),
    )
)
