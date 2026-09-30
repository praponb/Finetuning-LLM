from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy("balance", "id")
w2 = Window.orderBy(F.col("balance").desc_nulls_first())
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("shipping_fee").alias("min_order_shipping_fee"))

with_windows = (
    df_accounts
    .filter(F.col("account_type") != "LOAN")
    .select(
        "id",
        "customer_id",
        "opened_date",
        F.lead("balance").over(w).alias("next_balance"),
        F.row_number().over(w).alias("seq_no"),
        F.rank().over(w2).alias("balance_rank"),
    )
)

result = (
    with_windows
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "opened_date",
        "next_balance",
        "balance_rank",
        F.col("min_order_shipping_fee"),
    )
    .orderBy(F.col("next_balance").desc(), "balance_rank")
)
