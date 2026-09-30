from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "txn_type").orderBy(F.col("is_flagged_flag").asc_nulls_last())
w2 = Window.orderBy("txn_time", "id")
w3 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w4 = Window.orderBy("latest_rank", "id")
w5 = Window.partitionBy("account_id").orderBy(F.col("amount").desc())
w6 = Window.partitionBy("txn_type").orderBy(F.col("seq_no").desc_nulls_first(), "id")

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-07-09 14:00:00").cast("timestamp")) & F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_clean"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "inner")
    .filter(F.col("d.account_type") == "CHECKING")
    .select(
        "src.*",
        F.col("d.balance"),
        F.col("d.account_type"),
        F.col("d.opened_date"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .withColumn("next_amount", F.lead("amount", 1).over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("next_balance", F.lead("balance", 1).over(w4))
    .withColumn("amount_rank", F.rank().over(w5))
)

deduped_transactions = (
    metrics
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged_flag",
        "merchant_clean",
        "balance",
        "opened_date",
        "seq_no",
    )
    .orderBy(F.col("id").asc_nulls_last())
    .limit(5)
)
