from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select("status", F.coalesce("balance", F.lit(0)).alias("balance_filled"))
)
