from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("txn_time_weekofyear", "id")
w2 = Window.partitionBy("merchant")
w3 = Window.partitionBy("status", "account_status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_status").orderBy("opened_date", "id")
w5 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status").orderBy("opened_date", "id")

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.trim("txn_type").alias("clean_txn_type"),
        F.weekofyear("txn_time").alias("txn_time_weekofyear"),
    )
)

joined = (
    filtered
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.txn_time",
        "src.merchant",
        "src.is_flagged",
        "src.txn_time_weekofyear",
        F.col("d.opened_date"),
        F.col("d.status"),
    )
)

enriched = (
    joined
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        "opened_date",
        "status",
        F.ntile(4).over(w_running).alias("txn_time_weekofyear_bucket"),
        F.sum("txn_time_weekofyear").over(w2).alias("merchant_sum_txn_time_weekofyear"),
    )
)

enriched_transactions = (
    enriched
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.account_id",
        "s.opened_date",
        "s.status",
        F.col("b.status").alias("account_status"),
    )
)

result = (
    enriched_transactions
    .select(
        "id",
        "opened_date",
        "status",
        "account_status",
        F.first("status", ignorenulls=True).over(w3).alias("first_known_status"),
        F.row_number().over(w4).alias("event_seq"),
        F.last("status", ignorenulls=True).over(w5).alias("last_known_status"),
        F.row_number().over(w6).alias("row_seq"),
    )
)
