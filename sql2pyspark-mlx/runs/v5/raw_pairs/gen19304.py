from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("balance", "id")
w2 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("opened_date", "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

filtered = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-06-24").cast("date"))
    .select(
        "*",
        F.col("balance").cast("int").alias("balance_int"),
        F.coalesce("status", F.lit("OTHER")).alias("status_filled"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_type").isin(["FEE", "DEBIT", "REFUND"])), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left_anti")
)

enriched = (
    eligible
    .withColumn("first_known_balance_int", F.first("balance_int", ignorenulls=True).over(w2))
    .withColumn("prev_balance", F.lag("balance", 1).over(w3))
)

result = (
    enriched
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "balance_int",
        "first_known_balance_int",
        F.col("min_account_balance"),
    )
)
