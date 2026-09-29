from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["FROZEN", "OPEN"]))
    .select("status")
    .distinct()
    .orderBy("status")
    .limit(50)
)
