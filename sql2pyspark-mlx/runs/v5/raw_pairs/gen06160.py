from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_latest = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.balance", "s.opened_date", F.col("c.txn_type"))
)

result = (
    with_latest
    .select(
        "id",
        "opened_date",
        "txn_type",
        F.when(F.col("txn_type").rlike("^(CREDIT|DEBIT)$"), "PRIORITY").when(F.col("txn_type").rlike("^(REFUND|FEE)$"), "EXTENDED").otherwise("GROUP_B").alias("txn_type_zone"),
    )
)
