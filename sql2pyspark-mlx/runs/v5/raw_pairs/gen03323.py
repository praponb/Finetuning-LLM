from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy(F.col("balance_int").desc(), "id")
w2 = Window.orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("customer_id", "account_type").orderBy(F.col("balance_avg_5").desc(), "id")

base = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-07-20").cast("date"))
    .select("*", F.col("balance").cast("int").alias("balance_int"))
)

eligible = (
    base
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

with_windows = (
    eligible
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "balance_int",
        F.ntile(10).over(w).alias("balance_int_bucket"),
        F.avg("balance").over(w2).alias("balance_avg_5"),
    )
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "balance", "balance_int", "balance_int_bucket", "balance_avg_5")
)

eligible_accounts = (
    ranked
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < "2024-05-01"), "left_anti")
)

result = (
    eligible_accounts
    .select("id", "balance", "balance_int_bucket", "balance_avg_5")
    .orderBy(F.col("balance_avg_5").desc(), F.col("id").desc())
)
