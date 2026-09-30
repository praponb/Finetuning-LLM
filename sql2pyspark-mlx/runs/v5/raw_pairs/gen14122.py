from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("balance").asc_nulls_last())
w2 = Window.orderBy(F.col("balance").desc(), "id")
w3 = Window.orderBy("balance", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

filtered = (
    df_accounts
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-04-17")))
    .withColumn("opened_date_label", F.date_format("opened_date", "MMM yyyy"))
)

enriched = (
    filtered
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.balance",
        "src.opened_date",
        F.col("d.city"),
        F.col("d.signup_date"),
    )
)

enriched_accounts = (
    enriched
    .select(
        "id",
        "signup_date",
        F.percent_rank().over(w).alias("balance_percent_rank"),
        F.ntile(5).over(w2).alias("balance_bucket"),
        F.lead("balance", 1, 0).over(w3).alias("next_balance"),
    )
)

with_latest = (
    enriched_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time").alias("transaction_txn_time"))
)

result = with_latest.select("id", "balance_bucket", "next_balance", "transaction_txn_time")
