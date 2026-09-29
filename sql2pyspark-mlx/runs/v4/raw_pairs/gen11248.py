from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "CLOSED")
    .select("customer_id")
    .unionByName(
        df_accounts
        .filter(F.col("account_type").isin(["CHECKING", "LOAN"]))
        .select("customer_id")
    )
)
