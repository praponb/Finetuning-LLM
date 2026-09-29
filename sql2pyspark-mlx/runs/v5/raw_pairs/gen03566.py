from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc())
w2 = Window.partitionBy("customer_id").orderBy(F.col("balance").asc_nulls_first(), "id")
w3 = Window.orderBy("rank_in_group", "id")
w4 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("status", "customer_id").orderBy("balance")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

deduped = (
    df_accounts
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    latest
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "rank_in_group",
        F.row_number().over(w3).alias("event_seq"),
        F.avg("balance").over(w4).alias("balance_avg_2"),
        F.percent_rank().over(w5).alias("balance_percent_rank"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance_avg_2",
        "s.balance_percent_rank",
        F.col("c.amount").alias("transaction_amount"),
    )
)

result = (
    with_latest
    .select("id", "customer_id", "balance_percent_rank", "transaction_amount")
    .orderBy(F.col("id").desc(), "transaction_amount")
)
