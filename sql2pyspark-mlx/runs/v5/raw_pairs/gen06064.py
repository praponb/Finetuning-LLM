from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id")

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-06-06 23:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.hour("txn_time").alias("txn_time_hour"),
        F.hour("txn_time").alias("txn_time_hour_2"),
    )
)

with_dims = (
    valid
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "inner")
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.amount",
        "src.txn_time",
        "src.txn_time_hour",
        F.col("a.opened_date"),
        F.col("a.account_type"),
    )
)

base = (
    with_dims
    .filter(~F.col("account_type").eqNullSafe(F.lit("CREDIT")))
    .withColumn("txn_type_lower", F.lower("txn_type"))
    .withColumn("account_type_filled", F.coalesce("account_type", F.lit("N/A")))
)

filtered = (
    base
    .filter(F.col("txn_type").isin(["DEBIT", "REFUND"]))
    .select(
        "*",
        F.when(F.col("amount") >= 4300, "High").when(F.col("amount") >= 1000, "Medium").otherwise("Low").alias("amount_band"),
        F.coalesce("account_type", F.lit("UNKNOWN")).alias("account_type_filled_2"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time_hour",
        "opened_date",
        "account_type",
        "account_type_filled",
        "amount_band",
        F.lead("txn_time_hour").over(w).alias("next_txn_time_hour"),
        F.lag("txn_time_hour").over(w).alias("prev_txn_time_hour"),
        F.lead("amount").over(w).alias("next_amount"),
        F.row_number().over(w).alias("row_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "txn_type",
        "amount",
        "opened_date",
        "account_type_filled",
        "amount_band",
        "prev_txn_time_hour",
        "next_amount",
        "row_seq",
    )
    .orderBy(F.col("txn_type").desc_nulls_last(), "row_seq")
)
