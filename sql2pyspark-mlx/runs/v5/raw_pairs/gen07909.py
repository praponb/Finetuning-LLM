from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("account_id").orderBy("account_balance", "id")
w_ordered_2 = Window.partitionBy("account_id").orderBy(F.col("account_balance").desc())
w3 = Window.partitionBy("account_id").orderBy("account_balance_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("account_balance_2", "id")

with_dims = (
    df_transactions
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .filter(F.col("d.opened_date").between("2024-05-12", "2024-11-17"))
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.txn_time",
        "src.is_flagged",
        F.col("d.balance"),
    )
)

joined = (
    with_dims
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "left")
    .select("src.*", F.col("a.opened_date"), F.col("a.account_type"))
)

valid = (
    joined
    .filter(F.col("txn_time") > F.lit("2023-07-02").cast("date"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        "balance",
        "opened_date",
        F.lower("account_type").alias("account_type_lower"),
    )
)

with_dims_transactions = (
    valid
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.account_id",
        "e.is_flagged",
        "e.balance",
        "e.account_type_lower",
        F.col("d.balance").alias("account_balance"),
    )
)

enriched = (
    with_dims_transactions
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "inner")
    .select(
        "src.*",
        F.col("b.balance").alias("account_balance_2"),
        F.col("b.opened_date"),
    )
)

with_windows = (
    enriched
    .withColumn("next_account_balance_2", F.lead("account_balance_2", 1).over(w_ordered))
    .withColumn("account_balance_percent_rank", F.percent_rank().over(w_ordered_2))
    .withColumn("first_known_account_balance", F.first("account_balance", ignorenulls=True).over(w3))
    .withColumn("account_balance_2_bucket", F.ntile(3).over(w4))
)

result = (
    with_windows
    .alias("s")
    .join(df_accounts.alias("dim"), F.col("s.account_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["status", "account_type"], ["status"], []],
        F.col("dim.status").alias("status"),
        F.col("dim.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("s.balance")).alias("max_balance"),
        F.grouping_id().alias("grouping_level"),
    )
)
