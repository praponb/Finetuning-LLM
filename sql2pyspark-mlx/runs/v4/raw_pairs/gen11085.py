from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN"]) & F.month("opened_date").isin([1, 3, 5]))
    .select("balance", F.lower("status").alias("status_lower"))
)
