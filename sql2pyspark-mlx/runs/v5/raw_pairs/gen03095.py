from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("status", "account_type").orderBy("amount", "id")
w3 = Window.partitionBy("status").orderBy("balance", "id")
w4 = Window.partitionBy("account_type", "status")

with_latest = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.balance",
        "s.opened_date",
        "s.status",
        F.col("c.amount"),
    )
)

metrics = (
    with_latest
    .filter(F.col("opened_date") <= "2023-10-05")
    .withColumn("amount_bucket", F.ntile(5).over(w_ordered))
    .withColumn("balance_bucket", F.ntile(10).over(w3))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w4)), 4))
)

base = (
    metrics
    .filter(F.col("opened_date") >= F.lit("2023-09-07").cast("date"))
    .select("*", F.date_sub("opened_date", 62).alias("opened_date_minus_62d"))
)

kept = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.amount").between(2200, 3500)), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        "amount",
        "amount_bucket",
        "balance_bucket",
        "amount_share",
        "opened_date_minus_62d",
        F.when(F.col("status").rlike("^(FROZEN|CLOSED)$"), "GROUP_A").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("status").asc_nulls_last())
)
