from pyspark.sql import functions as F

result = (
    df_accounts
    .select("account_type", "status")
    .distinct()
    .orderBy(F.col("status").desc(), F.col("account_type").desc())
    .limit(20)
)
