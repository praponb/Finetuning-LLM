from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(51000, 77000))
    .select("account_type")
    .distinct()
)
