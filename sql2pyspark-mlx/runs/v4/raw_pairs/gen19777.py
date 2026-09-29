from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy(F.col("balance").asc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("account_type").orderBy("balance", "id")

with_windows = (
    df_accounts
    .select(
        "*",
        F.percent_rank().over(w).alias("balance_percent_rank"),
        F.sum("balance").over(w2).alias("cumulative_balance"),
        F.avg("balance").over(w3).alias("status_avg_balance"),
        F.lead("balance", 2).over(w4).alias("next_balance"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "balance_percent_rank",
        "cumulative_balance",
        "status_avg_balance",
        "next_balance",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
