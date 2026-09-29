from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") != "FROZEN") | F.col("status").isin(["FROZEN", "CLOSED", "OPEN"]))
    .select("id", "opened_date", "balance", "account_type")
    .orderBy("opened_date", "balance")
)
