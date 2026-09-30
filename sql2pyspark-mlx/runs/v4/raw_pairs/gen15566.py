from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2024-01-01", "2025-03-01"))
    .select("id", "status")
    .unionByName(df_accounts.filter(F.col("balance") >= 55000).select("id", "status"))
    .distinct()
    .orderBy(F.col("id").desc())
)
