from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

valid = (
    df_accounts
    .filter(F.col("customer_id").isin([2, 5, 9, 28]))
    .select(
        "*",
        F.round((F.col("balance") * 1.1), 2).alias("balance_adjusted"),
        F.when(F.col("balance") >= 75000, "High").when(F.col("balance") >= 10000, "Medium").otherwise("Low").alias("balance_band"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type").alias("transaction_txn_type"))
)
