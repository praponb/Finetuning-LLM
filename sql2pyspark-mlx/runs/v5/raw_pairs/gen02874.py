from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").asc_nulls_last(), "id")
w2 = Window.orderBy("amount")
w3 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("txn_type").orderBy("row_num", "id")
w5 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w6 = Window.partitionBy("merchant").orderBy("row_num", "id")
w_hist = Window.partitionBy("txn_type", "account_id").orderBy("running_amount", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("merchant").orderBy(F.col("next_amount").desc(), "id")
w_recent = Window.partitionBy("account_type").orderBy("last3_count_next_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_transactions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    ranked
    .filter(F.col("txn_type").isin(["DEBIT", "FEE", "CREDIT"]))
    .select(
        "*",
        F.cume_dist().over(w2).alias("amount_cume_dist"),
        F.sum("amount").over(w3).alias("running_amount"),
        F.lead("amount", 1).over(w4).alias("next_amount"),
        F.row_number().over(w5).alias("event_seq"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.sum("next_amount").over(w_hist).alias("last3_sum_next_amount"),
        F.count("next_amount").over(w_hist).alias("last3_count_next_amount"),
        F.min("next_amount").over(w_hist).alias("last3_min_next_amount"),
    )
)

ranked_transactions = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w8))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    ranked_transactions
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.account_id",
        "src.amount",
        "src.txn_time",
        "src.merchant",
        "src.is_flagged",
        "src.row_num",
        "src.amount_cume_dist",
        "src.running_amount",
        "src.last3_count_next_amount",
        "src.last3_min_next_amount",
        F.col("p.account_type"),
    )
)

with_dims = (
    joined
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "inner")
    .select("e.*", F.col("a.balance"))
)

with_windows_transactions = (
    with_dims
    .select(
        "*",
        F.min("running_amount").over(w_recent).alias("running_min_running_amount"),
        F.max("running_amount").over(w_recent).alias("running_max_running_amount"),
        F.sum("running_amount").over(w_recent).alias("running_sum_running_amount"),
        F.count("running_amount").over(w_recent).alias("running_count_running_amount"),
    )
)

result = (
    with_windows_transactions
    .groupBy(F.date_trunc("YEAR", "txn_time").alias("year_start"))
    .agg(
        F.max("txn_time").alias("ended_at"),
        F.max(F.when(F.col("merchant") == "Starbucks", F.col("amount"))).alias("starbucks_max_amount"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("span_minutes"),
    )
)
