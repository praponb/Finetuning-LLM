from pyspark.sql import functions as F

balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

flattened = (
    df_carts
    .select("id", "customer_id", F.explode("attributes").alias("k", "attr_value"))
)

result = (
    flattened
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select("id", "customer_id", "k", "attr_value", F.col("avg_account_balance"))
    .orderBy(F.col("customer_id").desc())
)
