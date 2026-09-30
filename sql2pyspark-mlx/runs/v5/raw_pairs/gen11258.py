from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "CREDIT", "CHECKING"]))
    .select("id", "status")
    .unionByName(
        df_accounts
        .filter(F.col("balance").between(38000, 61000))
        .select("id", "status")
    )
)
