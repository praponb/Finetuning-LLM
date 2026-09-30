from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("balance").asc_nulls_last())
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_last())
w3 = Window.orderBy("opened_date", "id").rowsBetween(-6, -1)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

enriched = (
    df_accounts
    .withColumn("balance_percent_rank", F.percent_rank().over(w))
    .withColumn("balance_percent_rank_2", F.percent_rank().over(w2))
    .withColumn("balance_avg_6_prior", F.avg("balance").over(w3))
)

result = (
    enriched
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "opened_date",
        "balance_percent_rank",
        "balance_percent_rank_2",
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("balance_percent_rank_2").asc_nulls_last(), "id")
    .limit(20)
)
