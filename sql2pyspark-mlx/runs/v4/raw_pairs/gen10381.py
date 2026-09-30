from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & F.col("account_type").isin(["CHECKING", "CREDIT", "SAVINGS"]) & (F.col("status") != "FROZEN"))
)

agg = (
    filtered_accounts
    .groupBy("account_type", "status")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.sum("balance").alias("total_balance"),
    )
)

result = (
    agg
    .select("account_type", "status", "latest_opened_date", "total_balance")
    .orderBy("account_type")
)
