from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy("balance", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("balance").asc_nulls_last())
w4 = Window.partitionBy("account_type", "status").orderBy(F.col("opened_date").asc_nulls_last(), "id")
w5 = Window.partitionBy("customer_id", "status")
w6 = Window.orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.partitionBy("status").orderBy("balance_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy("opened_date", "id")
w9 = Window.partitionBy("status").orderBy("customer_id_max_balance_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("status").orderBy("customer_id_max_balance_percent_rank", "id")
w_part = Window.partitionBy("status").orderBy(F.col("customer_id_max_balance_percent_rank").desc(), "id")

metrics = (
    df_accounts
    .filter(F.col("balance") > 73000)
    .withColumn("next_balance", F.lead("balance", 1).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("balance_percent_rank", F.percent_rank().over(w3))
)

ranked = (
    metrics
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        "next_balance",
        "balance_percent_rank",
    )
)

metrics_accounts = (
    ranked
    .select(
        "id",
        "status",
        "balance_percent_rank",
        F.max("balance_percent_rank").over(w5).alias("customer_id_max_balance_percent_rank"),
        F.avg("next_balance").over(w6).alias("next_balance_avg_3"),
        F.sum("next_balance").over(w_hist).alias("running_next_balance"),
        F.lead("next_balance", 2).over(w8).alias("next_next_balance"),
        F.first("status", ignorenulls=True).over(w_hist).alias("first_known_status"),
    )
)

scored = (
    metrics_accounts
    .select(
        "*",
        F.first("status", ignorenulls=True).over(w9).alias("first_known_status_2"),
        F.ntile(10).over(w_grp).alias("customer_id_max_balance_percent_rank_bucket"),
        F.sum("running_next_balance").over(w9).alias("running_running_next_balance"),
        F.ntile(10).over(w_part).alias("customer_id_max_balance_percent_rank_bucket_2"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.account_id").isin([4, 20, 37])), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "status",
        "balance_percent_rank",
        "next_next_balance",
        "customer_id_max_balance_percent_rank_bucket",
        "running_running_next_balance",
        "customer_id_max_balance_percent_rank_bucket_2",
        F.when(F.col("status").rlike("^(FROZEN|CLOSED)$"), "GROUP_A").when(F.col("status").rlike("^(OPEN)$"), "EXTENDED").otherwise("PRIORITY").alias("status_zone"),
    )
)
