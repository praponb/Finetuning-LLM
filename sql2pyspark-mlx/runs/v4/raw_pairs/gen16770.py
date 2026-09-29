from pyspark.sql import functions as F

monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

joined = (
    df_accounts
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.credit_limit"))
)

result = (
    joined
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        "credit_limit",
        F.when(F.col("account_type").rlike("^(LOAN|SAVINGS)$"), "GROUP_B").when(F.col("account_type").rlike("^(CHECKING|CREDIT)$"), "EXTENDED").otherwise("PRIORITY").alias("account_type_zone"),
        F.col("avg_subscription_monthly_fee"),
    )
)
