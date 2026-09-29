from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.orderBy(F.col("is_open").desc_nulls_last())
w3 = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w4 = Window.partitionBy("customer_id").orderBy("prev_balance", "id")
w5 = Window.partitionBy("status", "customer_id")
w6 = Window.partitionBy("account_type").orderBy(F.col("prev_balance_bucket").desc())
w7 = Window.partitionBy("account_type").orderBy(F.col("rank_in_group").desc(), "id")

valid = (
    df_accounts
    .filter(F.col("balance") <= 18000)
    .select(
        "*",
        F.when(F.col("status") == "OPEN", 1).otherwise(0).alias("is_open"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("resolved_status"),
    )
)

scored = (
    valid
    .filter(F.col("account_type") == "CHECKING")
    .select(
        "*",
        F.avg("is_open").over(w).alias("status_avg_is_open"),
        F.cume_dist().over(w2).alias("is_open_cume_dist"),
        F.lag("balance", 1).over(w3).alias("prev_balance"),
    )
)

scored_accounts = (
    scored
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "resolved_status",
        "status_avg_is_open",
        "prev_balance",
        F.ntile(4).over(w4).alias("prev_balance_bucket"),
        F.avg("status_avg_is_open").over(w5).alias("status_avg_status_avg_is_open"),
    )
)

ranked = (
    scored_accounts
    .withColumn("rank_in_group", F.rank().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

deduped = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.merchant") == "7-Eleven"), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "customer_id",
        "balance",
        "prev_balance",
        "prev_balance_bucket",
        "status_avg_status_avg_is_open",
        "rank_in_group",
        "latest_rank",
    )
    .orderBy(F.col("prev_balance").desc_nulls_last())
)
