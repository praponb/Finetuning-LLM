from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id", "account_type").orderBy("balance", "id")
w3 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-6, Window.currentRow)

with_windows = (
    df_accounts
    .select(
        "*",
        F.lag("balance", 1, 0).over(w).alias("prev_balance"),
        F.ntile(3).over(w2).alias("balance_bucket"),
        F.avg("balance").over(w3).alias("balance_avg_6"),
        F.avg("balance").over(w4).alias("balance_avg_6_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "prev_balance",
        "balance_avg_6",
        "balance_avg_6_2",
    )
    .orderBy(F.col("account_type").desc())
)
