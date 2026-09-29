from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["web", "store", "partner"]))
    .select("status")
    .unionByName(
        df_accounts
        .filter(F.col("account_type").isin(["CHECKING", "LOAN"]))
        .select("status")
    )
)
