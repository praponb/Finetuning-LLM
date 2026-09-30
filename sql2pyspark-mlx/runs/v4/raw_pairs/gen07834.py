from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([5, 12, 20]))
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("start_date") > "2025-08-26")
        .select("status")
    )
)
