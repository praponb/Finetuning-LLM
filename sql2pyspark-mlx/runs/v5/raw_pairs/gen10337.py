from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("account_id", "txn_type").orderBy("balance", "id")
w2 = Window.partitionBy("txn_type").orderBy("balance", "id")
w3 = Window.partitionBy("account_id").orderBy("merchant_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("next_balance", "id").rowsBetween(-5, -1)
w5 = Window.orderBy(F.col("next_merchant_length").desc_nulls_first())
w6 = Window.orderBy(F.col("row_seq_avg_5_prior").desc(), "id")
w7 = Window.orderBy("row_seq_avg_5_prior", "id")
w8 = Window.orderBy(F.col("running_row_seq").desc())
w9 = Window.orderBy(F.col("running_row_seq_dense_rank").desc(), "id")
w10 = Window.orderBy(F.col("running_row_seq").desc(), "id")

filtered = (
    df_transactions
    .filter((F.col("merchant").eqNullSafe(F.lit("Lazada"))) & (F.col("txn_time") >= F.lit("2023-05-17").cast("date")))
    .withColumn("merchant_length", F.length("merchant"))
)

with_dims = (
    filtered
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .filter(F.col("p.account_type") != "CREDIT")
    .select("s.*", F.col("p.account_type"), F.col("p.balance"))
)

with_windows = (
    with_dims
    .select(
        "*",
        F.lead("merchant_length").over(w_running).alias("next_merchant_length"),
        F.lead("balance").over(w_running).alias("next_balance"),
        F.row_number().over(w_running).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

with_dims_transactions = (
    with_windows
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "inner")
    .select(
        "src.*",
        F.col("b.opened_date"),
        F.col("b.account_type").alias("account_account_type"),
    )
)

scored = (
    with_dims_transactions
    .select(
        "id",
        "amount",
        "txn_time",
        "next_merchant_length",
        "next_balance",
        "seq_no",
        "row_seq",
        "opened_date",
        F.sum("row_seq").over(w3).alias("running_row_seq"),
        F.avg("row_seq").over(w4).alias("row_seq_avg_5_prior"),
    )
)

scored_transactions = (
    scored
    .select(
        "id",
        "amount",
        "next_merchant_length",
        "row_seq",
        "opened_date",
        "running_row_seq",
        "row_seq_avg_5_prior",
        F.cume_dist().over(w5).alias("next_merchant_length_cume_dist"),
        F.ntile(3).over(w6).alias("row_seq_avg_5_prior_bucket"),
        F.lag("next_merchant_length", 2).over(w7).alias("prev_next_merchant_length"),
        F.dense_rank().over(w8).alias("running_row_seq_dense_rank"),
    )
)

scored_transactions_2 = (
    scored_transactions
    .select(
        "id",
        "amount",
        "next_merchant_length",
        "row_seq",
        "running_row_seq",
        "next_merchant_length_cume_dist",
        F.ntile(10).over(w9).alias("running_row_seq_dense_rank_bucket"),
        F.ntile(4).over(w10).alias("running_row_seq_bucket"),
        F.ntile(5).over(w10).alias("running_row_seq_bucket_2"),
    )
)

result = (
    scored_transactions_2
    .select(
        "id",
        "next_merchant_length",
        "row_seq",
        "running_row_seq",
        "next_merchant_length_cume_dist",
        "running_row_seq_dense_rank_bucket",
        "running_row_seq_bucket",
    )
)
