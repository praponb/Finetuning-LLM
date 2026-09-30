from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc_nulls_first())
w_running = Window.partitionBy("merchant", "txn_type").orderBy(F.col("amount").desc(), "id")
w_grp = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("amount").asc_nulls_last())
w5 = Window.orderBy("amount", "id")

scored = (
    df_transactions
    .select(
        "*",
        F.rank().over(w).alias("amount_rank"),
        F.ntile(5).over(w_running).alias("amount_bucket"),
        F.last("amount", ignorenulls=True).over(w_grp).alias("last_known_amount"),
        F.percent_rank().over(w4).alias("amount_percent_rank"),
        F.lag("amount", 1).over(w5).alias("prev_amount"),
    )
)

result = (
    scored
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "amount_bucket",
        "last_known_amount",
        "amount_percent_rank",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
