from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("status").orderBy("balance", "id")
w3 = Window.partitionBy("account_type", "status").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_type").orderBy(F.col("running_balance").desc_nulls_last(), "id")
w_running = Window.partitionBy("account_type").orderBy("rn", "id").rowsBetween(-3, Window.currentRow)
w6 = Window.partitionBy("account_type").orderBy("last4_min_balance_bucket", "id")
w7 = Window.orderBy("last4_min_balance_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-02-27").cast("date"))
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.when(F.col("balance") >= 85000, "High").when(F.col("balance") >= 26000, "Medium").otherwise("Low").alias("balance_band"),
        F.coalesce("status", F.lit("OTHER")).alias("status_filled"),
    )
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

valid = (
    latest
    .filter(F.col("balance") > 16000)
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        "rn",
        F.concat("status", F.lit(" - "), "account_type").alias("status_account_type"),
    )
)

enriched = (
    valid
    .filter(F.col("balance") > 60000)
    .select(
        "id",
        "account_type",
        F.ntile(10).over(w2).alias("balance_bucket"),
        F.sum("balance").over(w3).alias("running_balance"),
        F.row_number().over(w).alias("event_seq"),
    )
)

latest_accounts = (
    enriched
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

with_windows = (
    latest_accounts
    .select(
        "id",
        "account_type",
        F.min("balance_bucket").over(w_running).alias("last4_min_balance_bucket"),
        F.max("balance_bucket").over(w_running).alias("last4_max_balance_bucket"),
        F.avg("balance_bucket").over(w_running).alias("last4_avg_balance_bucket"),
        F.count("balance_bucket").over(w_running).alias("last4_count_balance_bucket"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "last4_min_balance_bucket",
        "last4_max_balance_bucket",
        "last4_avg_balance_bucket",
        F.lag("last4_avg_balance_bucket").over(w6).alias("prev_last4_avg_balance_bucket"),
        F.lead("last4_avg_balance_bucket").over(w6).alias("next_last4_avg_balance_bucket"),
        F.lead("last4_max_balance_bucket").over(w6).alias("next_last4_max_balance_bucket"),
        F.row_number().over(w6).alias("row_seq"),
        F.first("last4_count_balance_bucket", ignorenulls=True).over(w7).alias("first_known_last4_count_balance_bucket"),
    )
)
