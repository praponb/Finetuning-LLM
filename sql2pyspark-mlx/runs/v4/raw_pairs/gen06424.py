from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

with_last = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant"), F.col("c.amount"))
)

result = (
    with_last
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        "merchant",
        "amount",
        F.col("min_order_shipping_fee"),
    )
    .orderBy(F.col("balance").desc())
)
