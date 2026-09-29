from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date_month_end").desc())
w2 = Window.orderBy("opened_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "account_type").orderBy(F.col("balance").desc(), "id")
w4 = Window.partitionBy("account_type").orderBy("cumulative_balance", "id")
w5 = Window.partitionBy("account_type").orderBy("cumulative_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("status").eqNullSafe(F.lit("OPEN"))))
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_clean"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (~F.col("x.is_flagged")), "left_anti")
)

not_flagged_accounts = (
    not_flagged
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") <= F.col("s.opened_date_month_end")), "left_semi")
)

latest = (
    not_flagged_accounts
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

eligible = (
    latest
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") < F.col("s.opened_date_month_end")), "left_anti")
)

with_windows = (
    eligible
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date_month_end",
        F.sum("balance").over(w2).alias("cumulative_balance"),
        F.ntile(10).over(w3).alias("balance_bucket"),
        F.ntile(4).over(w3).alias("balance_bucket_2"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date_month_end",
        "cumulative_balance",
        "balance_bucket_2",
        F.lag("balance", 1, 0).over(w4).alias("prev_balance"),
        F.last("cumulative_balance", ignorenulls=True).over(w5).alias("last_known_cumulative_balance"),
    )
)

summary = (
    metrics
    .groupBy(
        F.when(F.col("prev_balance") >= 81000, "PREMIUM").otherwise("STANDARD").alias("prev_balance_tier"),
        "account_type",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_rows"),
    )
    .filter(F.col("num_rows") > 1)
)

result = (
    summary
    .select(
        "prev_balance_tier",
        "account_type",
        "num_rows",
        F.when(F.col("account_type").rlike("^(SAVINGS|CHECKING)$"), "CORE").otherwise("OTHER_ZONE").alias("account_type_zone"),
    )
)
