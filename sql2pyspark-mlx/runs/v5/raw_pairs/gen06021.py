from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("t.merchant").isin(["Lazada", "Amazon"]) & (F.col("t.merchant") == "Lazada"))
    .select(
        F.col("t.account_id"),
        F.col("t.merchant"),
        F.col("a.status"),
        F.col("c.age"),
        F.col("t.amount").cast("int").alias("amount_int"),
    )
)
