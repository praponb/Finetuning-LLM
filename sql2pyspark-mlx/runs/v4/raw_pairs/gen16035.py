from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w4 = Window.orderBy("session_no")
w5 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.partitionBy("merchant").orderBy(F.col("session_no_percent_rank").asc_nulls_first())
w7 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w8 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-12-17 17:00:00").cast("timestamp"))
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.concat_ws("-", "merchant", "txn_type").alias("merchant_txn_type_key"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_clean"),
    )
)

gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

with_gaps = (
    islands
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

matched_keys = (
    sessions_transactions
    .select("merchant", "id")
    .subtract(
        df_transactions
        .filter(F.col("merchant") == "Grab")
        .select("merchant", "id")
    )
)

verified = (
    sessions_transactions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.merchant") == F.col("k.merchant")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.avg("is_flagged_flag").over(w3).alias("is_flagged_flag_avg_90d"),
        F.percent_rank().over(w4).alias("session_no_percent_rank"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.count("is_flagged_flag").over(w5).alias("is_flagged_flag_count_7d"),
        F.dense_rank().over(w6).alias("session_no_percent_rank_dense_rank"),
        F.sum("session_id").over(w7).alias("session_id_sum_90d"),
        F.sum("session_id").over(w8).alias("session_id_sum_30d"),
    )
)

result = (
    scored
    .groupBy(F.date_trunc("YEAR", "txn_time").alias("year_start"))
    .agg(
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.sum(F.when(F.col("merchant") == "Lazada", F.col("is_new_session")).otherwise(0)).alias("lazada_is_new_session"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes_2"),
        F.sum(F.when(F.col("merchant") == "Starbucks", F.col("session_id_sum_90d")).otherwise(0)).alias("starbucks_session_id_sum_90d"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("span_minutes_2").desc_nulls_last())
)
