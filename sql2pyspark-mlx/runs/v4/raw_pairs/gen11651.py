from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date").asc_nulls_last(), "id")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

deduped = (
    df_subscriptions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "latest_rank",
        F.col("max_account_balance"),
    )
    .orderBy(F.col("customer_id").asc_nulls_last())
)
