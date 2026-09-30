from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .unionByName(
        df_accounts
        .filter(F.col("account_type").isin(["SAVINGS", "CHECKING"]))
        .select("status")
    )
    .distinct()
)
