from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy(F.col("opened_date").desc())
w2 = Window.partitionBy("status", "account_type")
w3 = Window.orderBy("opened_date", "id").rowsBetween(-6, -1)
w4 = Window.orderBy(F.col("balance").asc_nulls_first())
w5 = Window.partitionBy("account_type", "status").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("discount").alias("sum_order_discount"))

latest = (
    df_accounts
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    latest
    .withColumn("status_avg_balance", F.avg("balance").over(w2))
    .withColumn("balance_avg_6_prior", F.avg("balance").over(w3))
    .withColumn("balance_percent_rank", F.percent_rank().over(w4))
    .withColumn("cumulative_balance", F.sum("balance").over(w5))
)

kept = (
    metrics
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date")), "left_anti")
)

result = (
    kept
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        "dedup_rank",
        "status_avg_balance",
        F.when(F.col("account_type").rlike("^(CHECKING|CREDIT)$"), "PRIORITY").otherwise("GROUP_A").alias("account_type_zone"),
        F.col("sum_order_discount"),
    )
)
