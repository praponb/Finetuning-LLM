from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(21000, 78000))
    .select("status")
    .unionByName(
        df_subscriptions
        .filter(F.col("customer_id").isNull())
        .select("status")
    )
    .distinct()
    .orderBy(F.col("status").desc())
)
