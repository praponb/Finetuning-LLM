from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant", "txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy("session_start_flag", "id").rowsBetween(-3, -1)
w5 = Window.orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy("txn_time", "id")
w7 = Window.partitionBy("account_id").orderBy(F.col("last_known_session_start_flag").desc_nulls_last())
w8 = Window.partitionBy("txn_type").orderBy("session_id", "id")
w9 = Window.partitionBy("txn_type").orderBy("session_start_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("txn_type").orderBy(F.col("amount_avg_3_prior").desc(), "id")

with_gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

scored = (
    sessions_transactions
    .select(
        "*",
        F.last("session_start_flag", ignorenulls=True).over(w3).alias("last_known_session_start_flag"),
        F.avg("amount").over(w4).alias("amount_avg_3_prior"),
        F.avg("session_start_flag").over(w5).alias("session_start_flag_avg_6"),
        F.lead("session_id", 2).over(w6).alias("next_session_id"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.rank().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "txn_type",
        "amount",
        "is_flagged",
        "session_start_flag",
        "session_id",
        "last_known_session_start_flag",
        "amount_avg_3_prior",
        "session_start_flag_avg_6",
        "next_session_id",
    )
)

scored_transactions = (
    ranked
    .select(
        "*",
        F.lag("session_start_flag_avg_6").over(w8).alias("prev_session_start_flag_avg_6"),
        F.lead("session_start_flag_avg_6").over(w8).alias("next_session_start_flag_avg_6"),
        F.lag("amount").over(w8).alias("prev_amount"),
        F.row_number().over(w8).alias("row_seq"),
        F.first("amount", ignorenulls=True).over(w9).alias("first_known_amount"),
    )
)

ranked_transactions = (
    scored_transactions
    .withColumn("latest_rank", F.row_number().over(w10))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    ranked_transactions
    .filter(F.col("amount").between(2500, 3900))
    .select(
        "id",
        "txn_type",
        "amount",
        "is_flagged",
        "session_start_flag",
        "session_id",
        "amount_avg_3_prior",
        "session_start_flag_avg_6",
        "next_session_id",
        "prev_session_start_flag_avg_6",
        "next_session_start_flag_avg_6",
        "prev_amount",
        "row_seq",
        "first_known_amount",
        "latest_rank",
        F.when(F.col("prev_session_start_flag_avg_6") >= 1, "High").when(F.col("prev_session_start_flag_avg_6") >= 0, "Medium").otherwise("Low").alias("prev_session_start_flag_avg_6_band"),
    )
)

result = (
    cleaned
    .groupBy("txn_type")
    .agg(
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_rows"),
        F.count(F.when(F.col("txn_type") == "DEBIT", 1)).alias("debit_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("amount_avg_3_prior", [0.25, 0.5, 0.75]).alias("amount_avg_3_prior_quartiles"),
    )
    .orderBy("txn_type")
)
