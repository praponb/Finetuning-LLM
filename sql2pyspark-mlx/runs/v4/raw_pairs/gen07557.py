from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["OPEN", "FROZEN"]))
    .select("id", "account_type", "status")
    .subtract(
        df_accounts
        .filter(F.col("customer_id") == 33)
        .select("id", "account_type", "status")
    )
    .orderBy("status")
)
