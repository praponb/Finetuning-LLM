from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id").rowsBetween(-3, -1)
w2 = Window.orderBy(F.col("opened_date").desc(), "id")
w3 = Window.orderBy("opened_date", "id")
w4 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id")
w6 = Window.partitionBy("customer_id").orderBy("first_known_balance", "id").rowsBetween(-3, Window.currentRow)
w_ordered = Window.partitionBy("customer_id").orderBy("opened_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_accounts
    .withColumn("balance_avg_3_prior", F.avg("balance").over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("prev_balance", F.lag("balance", 1).over(w3))
)

enriched_accounts = (
    enriched
    .filter(F.col("balance_avg_3_prior") <= 65323)
    .select(
        "id",
        "customer_id",
        "opened_date",
        "balance_avg_3_prior",
        "prev_balance",
        F.first("balance", ignorenulls=True).over(w4).alias("first_known_balance"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

scored = (
    enriched_accounts
    .select(
        "*",
        F.max("balance_avg_3_prior").over(w6).alias("last4_max_balance_avg_3_prior"),
        F.min("balance_avg_3_prior").over(w6).alias("last4_min_balance_avg_3_prior"),
        F.count("balance_avg_3_prior").over(w6).alias("last4_count_balance_avg_3_prior"),
    )
)

with_window = (
    scored
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.opened_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    with_window
    .select(
        "id",
        "opened_date",
        "balance_avg_3_prior",
        "prev_balance",
        "first_known_balance",
        "row_seq",
        "last4_max_balance_avg_3_prior",
        "last4_min_balance_avg_3_prior",
        "last4_count_balance_avg_3_prior",
        "subscription_id",
        "monthly_fee",
        F.max("balance_avg_3_prior").over(w_ordered).alias("running_max_balance_avg_3_prior"),
        F.sum("balance_avg_3_prior").over(w_ordered).alias("running_sum_balance_avg_3_prior"),
        F.min("balance_avg_3_prior").over(w_ordered).alias("running_min_balance_avg_3_prior"),
        F.avg("balance_avg_3_prior").over(w_ordered).alias("running_avg_balance_avg_3_prior"),
    )
)
