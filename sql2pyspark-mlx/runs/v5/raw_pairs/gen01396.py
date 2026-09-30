from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").asc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

filtered = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "SAVINGS", "CREDIT"]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
    )
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_latest = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.account_type",
        "s.balance",
        "s.opened_date",
        "s.opened_date_label",
        F.col("c.txn_time"),
    )
)

joined = (
    with_latest
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.credit_limit"))
)

wide = (
    joined
    .groupBy("customer_id")
    .pivot("account_type", ["CHECKING", "SAVINGS", "CREDIT"])
    .agg(F.max("balance").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)

result = (
    wide
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "CHECKING_amount",
        "CHECKING_cnt",
        "SAVINGS_amount",
        "SAVINGS_cnt",
        "CREDIT_amount",
        "CREDIT_cnt",
        F.col("sum_subscription_monthly_fee"),
    )
)
