from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("account_type").orderBy("balance", "id")
w3 = Window.partitionBy("status").orderBy("balance", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_windows = (
    df_accounts
    .filter(F.col("status") == "OPEN")
    .select(
        "*",
        F.avg("balance").over(w).alias("balance_avg_3"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("seq_no_2"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time"), F.col("c.amount").alias("transaction_amount"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

not_flagged_accounts = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.txn_time")), "left_anti")
)

result = (
    not_flagged_accounts
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "balance_avg_3",
        "seq_no",
        F.when(F.col("account_type").rlike("^(LOAN|CHECKING)$"), "GROUP_B").when(F.col("account_type").rlike("^(SAVINGS|CREDIT)$"), "OTHER_ZONE").otherwise("CORE").alias("account_type_zone"),
    )
    .orderBy(F.col("id").desc(), "customer_id")
)
