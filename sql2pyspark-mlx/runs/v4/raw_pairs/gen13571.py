from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

with_last = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.amount").alias("transaction_amount"),
        F.col("c.merchant").alias("transaction_merchant"),
    )
)

result = (
    with_last
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.when(F.col("account_type").rlike("^(SAVINGS|CHECKING)$"), "OTHER_ZONE").when(F.col("account_type").rlike("^(CREDIT|LOAN)$"), "GROUP_A").otherwise("PRIORITY").alias("account_type_zone"),
        F.col("min_subscription_monthly_fee"),
    )
)
