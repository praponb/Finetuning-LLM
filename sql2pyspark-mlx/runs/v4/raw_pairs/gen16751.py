from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").asc_nulls_first())
w2 = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee", "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

with_windows = (
    df_subscriptions
    .withColumn("monthly_fee_cume_dist", F.cume_dist().over(w))
    .withColumn("prev_monthly_fee", F.lag("monthly_fee", 2, 0).over(w2))
)

result = (
    with_windows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "monthly_fee_cume_dist",
        "prev_monthly_fee",
        F.col("avg_account_balance"),
    )
)
