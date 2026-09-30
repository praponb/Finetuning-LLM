from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "CREDIT")
    .select("status")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("status")
    )
    .orderBy("status")
)
