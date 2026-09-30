from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .distinct()
    .orderBy(F.col("status").desc())
    .limit(10)
)
