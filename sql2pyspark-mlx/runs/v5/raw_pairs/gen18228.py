from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

ranked = (
    df_accounts
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "row_num",
        F.when(F.col("status").rlike("^(OPEN|FROZEN)$"), "GROUP_A").when(F.col("status").rlike("^(CLOSED)$"), "PRIORITY").otherwise("EXTENDED").alias("status_zone"),
        F.col("max_subscription_monthly_fee"),
    )
)
