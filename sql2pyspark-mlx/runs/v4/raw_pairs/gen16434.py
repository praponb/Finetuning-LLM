from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-04-08").cast("date")) & F.col("customer_id").isNotNull())
    .withColumn("account_type_lower", F.lower("account_type"))
    .withColumn("account_type_prefix", F.substring("account_type", 1, 4))
)

result = (
    base
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.txn_time") >= F.col("s.opened_date")), "left_anti")
)
