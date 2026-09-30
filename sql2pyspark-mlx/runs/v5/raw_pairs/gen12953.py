from pyspark.sql import functions as F

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("merchant_filled", F.coalesce("merchant", F.lit("unassigned")))
)

result = (
    cleaned
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.account_type"))
)
