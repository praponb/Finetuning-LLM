from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

cleaned = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.upper("status").alias("status_upper"),
        F.when(F.col("account_type").isin(["LOAN", "CHECKING"]), "Core").otherwise("Other").alias("account_type_group"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.balance",
        "s.opened_date",
        "s.status",
        "s.account_type_group",
        F.col("c.txn_time").alias("transaction_txn_time"),
        F.col("c.txn_type"),
    )
)
