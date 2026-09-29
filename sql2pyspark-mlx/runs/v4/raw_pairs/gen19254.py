from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "CLOSED")
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("plan") != "enterprise")
        .select("status")
    )
)
