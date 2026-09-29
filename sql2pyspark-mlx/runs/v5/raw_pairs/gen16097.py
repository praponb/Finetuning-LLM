from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("merchant").orderBy("amount", "id")
w_ordered_2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("account_id", "txn_type")

scored = (
    df_transactions
    .filter(F.col("txn_type") == "DEBIT")
    .select(
        "*",
        F.ntile(3).over(w_ordered).alias("amount_bucket"),
        F.last("merchant", ignorenulls=True).over(w_ordered_2).alias("last_known_merchant"),
    )
)

remaining_keys = (
    scored
    .select("account_id")
    .intersectAll(
        df_transactions
        .filter(F.col("amount") < 2100)
        .select("account_id")
    )
)

verified = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        "amount_bucket",
        "last_known_merchant",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

scored_transactions = (
    filtered
    .select(
        "id",
        "account_id",
        "last_known_merchant",
        "is_flagged_label",
        F.max("amount_bucket").over(w3).alias("amount_bucket_max_7d"),
        F.min("amount_bucket").over(w4).alias("account_id_min_amount_bucket"),
    )
)

result = (
    scored_transactions
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.amount_bucket_max_7d")), 2).alias("total_amount_bucket_max_7d"),
        F.sum(F.col("a.account_id_min_amount_bucket")).alias("total_account_id_min_amount_bucket"),
    )
)
