from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .unionByName(
        df_accounts
        .filter(F.col("balance").between(66000, 70000))
        .select("status")
    )
)
