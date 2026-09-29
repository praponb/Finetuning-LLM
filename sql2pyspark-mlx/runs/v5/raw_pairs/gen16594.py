from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "LOAN") & (F.col("status").isin(["OPEN", "FROZEN"]) | F.col("status").isin(["FROZEN", "OPEN", "CLOSED"])))
    .select("id", "status", F.last_day("opened_date").alias("opened_date_month_end"))
    .orderBy(F.col("status").desc())
)
