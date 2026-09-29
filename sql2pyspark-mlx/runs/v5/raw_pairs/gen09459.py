from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc_nulls_first())
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status")
w7 = Window.partitionBy("customer_id", "status").orderBy(F.col("balance_avg_6_prior").desc_nulls_first())
w_running = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w_hist = Window.partitionBy("status").orderBy("balance_percent_rank")
w10 = Window.partitionBy("account_type", "status").orderBy(F.col("balance_percent_rank").desc(), "id")

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date")), "left_semi")
)

scored = (
    not_flagged
    .withColumn("balance_percent_rank", F.percent_rank().over(w))
    .withColumn("customer_id_max_balance", F.max("balance").over(w2))
    .withColumn("balance_avg_6", F.avg("balance").over(w3))
    .withColumn("balance_avg_6_prior", F.avg("balance").over(w4))
    .withColumn("first_known_balance", F.first("balance", ignorenulls=True).over(w5))
)

enriched = (
    scored
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "customer_id_max_balance",
        "balance_avg_6",
        "balance_avg_6_prior",
        "first_known_balance",
        F.max("first_known_balance").over(w6).alias("status_max_first_known_balance"),
        F.cume_dist().over(w7).alias("balance_avg_6_prior_cume_dist"),
        F.lag("customer_id_max_balance", 1, 0).over(w_running).alias("prev_customer_id_max_balance"),
        F.percent_rank().over(w_hist).alias("balance_percent_rank_percent_rank"),
        F.row_number().over(w10).alias("seq_no"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

kept_accounts = (
    kept
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_type") == "DEBIT"), "left_anti")
)

result = (
    kept_accounts
    .alias("s")
    .join(df_customers.alias("dim"), F.col("s.customer_id") == F.col("dim.id"), "left")
    .groupBy(F.col("dim.city").alias("city"), F.col("s.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("s.balance_avg_6_prior_cume_dist")).alias("avg_balance_avg_6_prior_cume_dist"),
    )
    .orderBy(F.col("num_rows").asc_nulls_first())
)
