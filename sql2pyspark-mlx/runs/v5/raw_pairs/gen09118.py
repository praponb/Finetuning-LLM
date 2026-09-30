from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(F.round(F.stddev("balance"), 2).alias("stddev_balance"))
    .filter(F.col("stddev_balance") < 34)
    .orderBy(F.col("stddev_balance").desc())
)
