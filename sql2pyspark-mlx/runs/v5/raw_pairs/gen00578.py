from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-04-01", "2025-07-01"))
    .select("id", "account_type", "status")
    .orderBy(F.col("status").desc())
)
