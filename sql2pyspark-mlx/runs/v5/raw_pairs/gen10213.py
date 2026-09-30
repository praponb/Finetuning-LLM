from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("amount", "id")
w3 = Window.partitionBy("account_id").orderBy(F.col("amount").desc_nulls_last())
w4 = Window.partitionBy("account_id").orderBy(F.col("amount_cume_dist").desc(), "id")

base = (
    df_transactions
    .filter(F.col("account_id").isin([8, 11]) & (F.col("txn_time") >= F.lit("2023-09-02").cast("date")))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.initcap(F.trim("merchant")).alias("merchant_title"),
    )
)

latest = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "left")
    .filter(F.col("d.opened_date") > "2025-06-01")
    .select(
        "e.id",
        "e.account_id",
        "e.amount",
        "e.merchant",
        "e.is_flagged",
        "e.merchant_title",
        F.col("d.opened_date"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "account_id",
        F.ntile(4).over(w2).alias("amount_bucket"),
        F.cume_dist().over(w3).alias("amount_cume_dist"),
        F.lead("amount", 1, 0).over(w2).alias("next_amount"),
    )
)

latest_transactions = (
    with_windows
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

result = (
    latest_transactions
    .select("id", "account_id", "next_amount")
    .orderBy(F.col("account_id").desc(), F.col("next_amount").desc())
)
