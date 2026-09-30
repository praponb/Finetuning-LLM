from pyspark.sql import functions as F

valid = (
    df_transactions
    .filter(F.col("txn_time") > F.lit("2023-07-04").cast("date"))
    .withColumn("merchant_upper", F.upper("merchant"))
)

result = (
    valid
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "left")
    .filter(F.col("p.account_type").isin(["CHECKING", "SAVINGS", "CREDIT"]))
    .select("src.*", F.col("p.status"), F.col("p.balance"))
)
