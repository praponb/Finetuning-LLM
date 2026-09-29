from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("balance", "id").rowsBetween(-2, -1)
w_recent_2 = Window.partitionBy("status", "customer_id").orderBy("balance", "id")
w3 = Window.partitionBy("status").orderBy(F.col("balance").asc_nulls_first())
w4 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_type", "status").orderBy("balance", "id")

enriched = (
    df_accounts
    .select(
        "*",
        F.avg("balance").over(w_recent).alias("balance_avg_2_prior"),
        F.row_number().over(w_recent_2).alias("row_seq"),
        F.rank().over(w3).alias("balance_rank"),
        F.first("status", ignorenulls=True).over(w4).alias("first_known_status"),
        F.ntile(3).over(w5).alias("balance_bucket"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_type") == "DEBIT"), "left_anti")
)

remaining_keys = (
    not_flagged
    .select("account_type", "customer_id")
    .subtract(
        df_accounts
        .filter(F.col("status") == "CLOSED")
        .select("account_type", "customer_id")
    )
)

kept_rows = (
    not_flagged
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "customer_id",
        "account_type",
        "balance",
        "status",
        "row_seq",
        "balance_rank",
        "first_known_status",
        "balance_bucket",
        F.when(F.col("account_type").rlike("^(CREDIT|CHECKING)$"), "OTHER_ZONE").otherwise("CORE").alias("account_type_zone"),
    )
    .orderBy(F.col("status").desc_nulls_last())
)
