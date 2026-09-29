from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("customer_id").orderBy("balance", "id")
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w3 = Window.partitionBy("account_type", "status").orderBy("balance_bucket", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("balance_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.orderBy("next_balance", "id")
w7 = Window.partitionBy("status")
w8 = Window.partitionBy("account_type").orderBy("balance_bucket", "id")
w9 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("status", "account_type").orderBy("next_balance_bucket", "id")
w11 = Window.partitionBy("account_type", "status").orderBy(F.col("next_balance_avg_5").desc(), "id")
w12 = Window.partitionBy("account_type").orderBy("balance", "id")
w13 = Window.partitionBy("status").orderBy("next_balance_bucket", "id")

scored = (
    df_accounts
    .select(
        "*",
        F.ntile(10).over(w_hist).alias("balance_bucket"),
        F.lead("balance", 2).over(w2).alias("next_balance"),
    )
)

with_windows = (
    scored
    .filter(F.col("account_type").isin(["SAVINGS", "CREDIT"]))
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        "balance_bucket",
        F.avg("next_balance").over(w3).alias("next_balance_avg_5"),
        F.sum("next_balance").over(w4).alias("cumulative_next_balance"),
        F.avg("next_balance").over(w5).alias("next_balance_avg_5_2"),
        F.ntile(4).over(w6).alias("next_balance_bucket"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.sum("next_balance_bucket").over(w7).alias("status_sum_next_balance_bucket"),
        F.row_number().over(w8).alias("seq_no"),
        F.sum("next_balance_avg_5").over(w9).alias("running_next_balance_avg_5"),
        F.ntile(4).over(w10).alias("next_balance_bucket_bucket"),
    )
)

metrics_accounts = (
    metrics
    .select(
        "*",
        F.row_number().over(w11).alias("event_seq"),
        F.row_number().over(w12).alias("seq_no_2"),
        F.lag("balance_bucket", 2).over(w13).alias("prev_balance_bucket"),
    )
)

result = (
    metrics_accounts
    .select(
        "id",
        "balance",
        "status",
        "balance_bucket",
        "next_balance_avg_5",
        "cumulative_next_balance",
        "next_balance_avg_5_2",
        "status_sum_next_balance_bucket",
        "seq_no",
        F.when(F.col("status").rlike("^(CLOSED|OPEN)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("status_zone"),
    )
    .orderBy(F.col("seq_no").desc())
)
