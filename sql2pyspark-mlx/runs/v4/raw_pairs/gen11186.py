from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "CHECKING", "CREDIT"]) & (F.col("account_type") == "CHECKING"))
    .orderBy("account_type", "id")
    .limit(5)
)
