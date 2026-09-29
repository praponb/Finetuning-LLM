from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("order_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("customer_id", "channel")
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.min("balance").alias("min_account_balance"))

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-03-26").cast("date")) & F.col("store_id").isNotNull() & (F.col("channel") == "web"))
    .withColumn("greatest_shipping_fee_discount", F.greatest("shipping_fee", "discount"))
)

with_windows = (
    valid
    .withColumn("amount_avg_5", F.avg("amount").over(w))
    .withColumn("corr_amount_discount", F.corr("amount", "discount").over(w2))
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price").between(140, 360)), "left_anti")
)

result = (
    not_flagged
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        "amount_avg_5",
        "corr_amount_discount",
        F.col("min_account_balance"),
    )
)
