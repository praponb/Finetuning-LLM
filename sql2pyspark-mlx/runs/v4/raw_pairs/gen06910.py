from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time_period", "id")
w2 = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")
w3 = Window.partitionBy("txn_type", "account_id")
w4 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w5 = Window.partitionBy("txn_type").orderBy(F.col("rank_in_group").desc(), "id")
w6 = Window.orderBy(F.col("seq_no").desc_nulls_first())
w7 = Window.partitionBy("merchant", "txn_type")

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("amount") >= 2900))
    .select(
        "*",
        F.date_format("txn_time", "MMM yyyy").alias("txn_time_label"),
        F.date_trunc("WEEK", "txn_time").alias("txn_time_period"),
    )
)

ranked = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.count("amount").over(w3).alias("txn_type_count_amount"),
        F.lag("amount", 1).over(w4).alias("prev_amount"),
    )
)

ranked_transactions = (
    with_windows
    .withColumn("rank_in_group_2", F.row_number().over(w5))
    .filter(F.col("rank_in_group_2") == 1)
    .drop("rank_in_group_2")
)

enriched = (
    ranked_transactions
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.opened_date"), F.col("d.balance"), F.col("d.status"))
)

metrics = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w6).alias("seq_no_percent_rank"),
        F.round((F.col("txn_type_count_amount") / F.sum("txn_type_count_amount").over(w7)), 4).alias("txn_type_count_amount_share"),
    )
)

result = (
    metrics
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "txn_time_label",
        "rank_in_group",
        "seq_no",
        "txn_type_count_amount",
    )
)
