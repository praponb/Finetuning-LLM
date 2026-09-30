from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

latest_match = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_time"))
)

result = (
    latest_match
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        "txn_time",
        F.when(F.col("status").rlike("^(FROZEN|OPEN)$"), "EXTENDED").when(F.col("status").rlike("^(CLOSED)$"), "GROUP_B").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("status").desc_nulls_first())
)
