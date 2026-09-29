from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "CHECKING", "LOAN"]))
    .select("id", "customer_id", "status")
    .orderBy("id")
)
