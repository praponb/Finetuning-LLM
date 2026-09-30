from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNull() | (F.col("status") == "FROZEN"))
    .select("id", "opened_date")
    .orderBy(F.col("id").desc())
)
