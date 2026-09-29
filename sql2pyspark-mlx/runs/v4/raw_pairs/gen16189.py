from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("account_type")
w3 = Window.orderBy(F.col("transaction_amount").asc_nulls_first())
w4 = Window.orderBy(F.col("balance").desc_nulls_last())
w5 = Window.partitionBy("customer_id", "account_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

base = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-09-03").cast("date"))
    .withColumn("status_title", F.initcap(F.trim("status")))
    .withColumn("status_clean", F.coalesce("status", F.lit("OTHER")))
)

with_last = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time"), F.col("c.amount").alias("transaction_amount"))
)

kept_keys = (
    with_last
    .select("account_type", "id")
    .subtract(
        df_accounts
        .filter(F.col("balance").between(28000, 51000))
        .select("account_type", "id")
    )
)

verified = (
    with_last
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .select(
        "*",
        F.sum("balance").over(w_recent).alias("account_type_sum_balance"),
        F.avg("transaction_amount").over(w_recent).alias("account_type_avg_transaction_amount"),
        F.dense_rank().over(w3).alias("transaction_amount_dense_rank"),
        F.rank().over(w4).alias("balance_rank"),
        F.max("balance").over(w5).alias("balance_max_90d"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("txn_time") < "2023-03-05")
    .withColumn("account_type_avg_transaction_amount_rounded", F.round("account_type_avg_transaction_amount", 2))
    .withColumn("balance_filled", F.coalesce("balance", F.lit(0)))
    .withColumn("status_filled", F.coalesce("status", F.lit("OTHER")))
)

result = (
    cleaned
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "transaction_amount",
        "balance_rank",
        "balance_max_90d",
        F.when(F.col("account_type").rlike("^(SAVINGS|LOAN)$"), "GROUP_A").when(F.col("account_type").rlike("^(CHECKING|CREDIT)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("account_type_zone"),
        F.col("max_account_balance"),
    )
)
