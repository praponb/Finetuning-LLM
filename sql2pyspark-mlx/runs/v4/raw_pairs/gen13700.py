from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())

filtered = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & F.col("balance").isNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.concat("account_type", F.lit("/"), "status").alias("account_type_status"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant"), F.col("c.txn_time"))
)
