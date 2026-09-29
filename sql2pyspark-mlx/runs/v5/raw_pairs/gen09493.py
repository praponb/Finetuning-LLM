from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_first())
w2 = Window.orderBy("monthly_fee", "id")
w3 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

with_windows = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(90, 190))
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        F.rank().over(w).alias("monthly_fee_rank"),
        F.ntile(3).over(w2).alias("monthly_fee_bucket"),
        F.first("monthly_fee", ignorenulls=True).over(w3).alias("first_known_monthly_fee"),
    )
)

result = (
    with_windows
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        "monthly_fee_rank",
        "monthly_fee_bucket",
        F.col("avg_account_balance"),
    )
    .orderBy(F.col("customer_id").asc_nulls_last(), F.col("monthly_fee_bucket").desc())
)
