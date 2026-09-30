from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-03-01", "2024-06-04"))
    .groupBy("account_type")
    .pivot("status", ["FROZEN", "CLOSED"])
    .agg(F.max("balance"))
    .orderBy("account_type")
)
