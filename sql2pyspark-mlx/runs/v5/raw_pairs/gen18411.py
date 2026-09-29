from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("amount").alias("min_order_amount"))

latest_match = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        "s.status",
        F.col("c.merchant"),
    )
)

result = (
    latest_match
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        "merchant",
        F.col("min_order_amount"),
    )
    .orderBy("merchant", "id")
)
