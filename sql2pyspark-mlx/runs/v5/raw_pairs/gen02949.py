from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-03-22").cast("date"))
    .withColumn("is_frozen", F.when(F.col("status") == "FROZEN", 1).otherwise(0))
)

result = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_anti")
)
