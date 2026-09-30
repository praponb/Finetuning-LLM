from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("customer_id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

enriched = (
    df_accounts
    .withColumn("last_known_status", F.last("status", ignorenulls=True).over(w))
    .withColumn("last_known_balance", F.last("balance", ignorenulls=True).over(w2))
    .withColumn("customer_id_max_balance", F.max("balance").over(w_part))
)

result = (
    enriched
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "last_known_status",
        F.when(F.col("account_type").rlike("^(CREDIT|CHECKING)$"), "CORE").when(F.col("account_type").rlike("^(SAVINGS|LOAN)$"), "GROUP_B").otherwise("EXTENDED").alias("account_type_zone"),
        F.col("min_subscription_monthly_fee"),
    )
)
