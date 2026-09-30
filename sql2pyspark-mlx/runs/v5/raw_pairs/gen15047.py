from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id")
w2 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("opened_date", "id")
w4 = Window.partitionBy("account_id").orderBy("prev_amount", "id")
w5 = Window.partitionBy("account_id")
w6 = Window.orderBy("account_id_count_prev_amount", "id")
w7 = Window.orderBy("prev_next_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(~F.col("merchant").eqNullSafe(F.lit("Amazon")))
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "is_flagged",
        F.concat_ws(",", "merchant", "txn_type").alias("merchant_txn_type_key"),
    )
)

with_dims = (
    base
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.account_type"),
        F.col("d.balance"),
        F.col("d.opened_date"),
    )
)

scored = (
    with_dims
    .filter(F.to_date("txn_time") >= "2025-05-01")
    .select(
        "id",
        "account_id",
        "is_flagged",
        "merchant_txn_type_key",
        F.lead("amount", 2).over(w).alias("next_amount"),
        F.avg("amount").over(w2).alias("amount_avg_90d"),
        F.lag("amount", 1).over(w3).alias("prev_amount"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "merchant_txn_type_key",
        F.lag("next_amount").over(w4).alias("prev_next_amount"),
        F.lead("amount_avg_90d").over(w4).alias("next_amount_avg_90d"),
        F.lag("amount_avg_90d").over(w4).alias("prev_amount_avg_90d"),
        F.row_number().over(w4).alias("event_seq"),
        F.count("prev_amount").over(w5).alias("account_id_count_prev_amount"),
    )
)

valid = (
    with_windows
    .filter(F.col("next_amount_avg_90d") < 1359)
    .select(
        "id",
        "merchant_txn_type_key",
        "prev_next_amount",
        "prev_amount_avg_90d",
        "account_id_count_prev_amount",
        F.ceil("prev_next_amount").alias("prev_next_amount_whole"),
    )
)

with_windows_transactions = (
    valid
    .select(
        "*",
        F.lag("prev_next_amount_whole").over(w6).alias("prev_prev_next_amount_whole"),
        F.lead("prev_amount_avg_90d").over(w6).alias("next_prev_amount_avg_90d"),
        F.row_number().over(w6).alias("row_seq"),
        F.first("prev_next_amount", ignorenulls=True).over(w7).alias("first_known_prev_next_amount"),
    )
)

result = (
    with_windows_transactions
    .select("id", "prev_next_amount", "first_known_prev_next_amount")
    .unpivot(
        ids=["id"],
        values=["prev_next_amount", "first_known_prev_next_amount"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
