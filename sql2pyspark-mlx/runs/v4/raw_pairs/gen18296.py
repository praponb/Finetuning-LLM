from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.max("balance").alias("max_account_balance"))

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_anti")
)

result = (
    eligible
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "browser", F.col("max_account_balance"))
    .orderBy(F.col("customer_id").desc())
)
