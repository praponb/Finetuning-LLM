from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2024-02-08"))) & F.col("status").isNotNull())
    .withColumn("account_type_lower", F.lower("account_type"))
)

result = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.account_id").isin([16, 29, 37])), "left_anti")
)
