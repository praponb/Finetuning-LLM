from pyspark.sql import functions as F

result = (
    df_accounts
    .select("account_type")
    .distinct()
    .orderBy(F.col("account_type").desc())
)
