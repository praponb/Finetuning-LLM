from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("monthly_fee").between(90, 200))
        .select("status")
    )
    .distinct()
)
