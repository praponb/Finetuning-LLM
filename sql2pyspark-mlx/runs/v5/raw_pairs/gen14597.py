from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status", "account_type").orderBy(F.col("txn_time").desc(), "id")

valid = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-03-14").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        F.when(F.col("balance") >= 64000, "High").when(F.col("balance") >= 21000, "Medium").otherwise("Low").alias("balance_tier"),
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.status",
        "s.status_filled",
        F.col("c.txn_time"),
        F.col("c.amount"),
    )
)

latest = (
    with_last
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

kept = (
    latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.is_flagged")), "left_semi")
)

result = kept.select("id", "account_type", "status", "txn_time")
