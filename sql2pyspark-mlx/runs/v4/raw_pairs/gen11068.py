from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("customer_id", "status").orderBy(F.col("amount").desc_nulls_first())
w3 = Window.partitionBy("customer_id").orderBy(F.col("amount_with_shipping").desc(), "id")
w4 = Window.partitionBy("status", "customer_id").orderBy("payment_amount", "id")
w_grp_2 = Window.partitionBy("status").orderBy("amount_with_shipping", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("status").orderBy("payment_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
balance_by_customer_id = df_accounts.groupBy("customer_id").agg(F.avg("balance").alias("avg_account_balance"))

base = (
    df_orders
    .filter(F.col("amount") > 930)
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "amount",
        (F.col("amount") + F.col("shipping_fee")).alias("amount_with_shipping"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("payment_amount"))
)

scored = (
    latest_match
    .withColumn("amount_cume_dist", F.cume_dist().over(w_grp))
    .withColumn("amount_with_shipping_bucket", F.ntile(3).over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("running_amount_with_shipping", F.sum("amount_with_shipping").over(w_grp_2))
    .withColumn("cumulative_amount_with_shipping", F.sum("amount_with_shipping").over(w_recent))
)

latest_match_orders = (
    scored
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("payment_status"))
)

joined = (
    latest_match_orders
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.country"), F.col("p.signup_date"), F.col("p.age"))
)

latest_match_orders_2 = (
    joined
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost").alias("shipment_cost"))
)

result = (
    latest_match_orders_2
    .join(balance_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "order_date",
        "status",
        "amount_with_shipping",
        "seq_no",
        "running_amount_with_shipping",
        "payment_status",
        "signup_date",
        F.col("avg_account_balance"),
    )
)
