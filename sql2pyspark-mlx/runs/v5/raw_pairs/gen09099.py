from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id")
w2 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "LOAN"]))
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
        F.substring("status", 1, 3).alias("status_prefix"),
    )
)

result = (
    base
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "balance_filled",
        F.lag("balance").over(w).alias("prev_balance"),
        F.lag("balance_filled").over(w).alias("prev_balance_filled"),
        F.row_number().over(w).alias("row_seq"),
        F.sum("balance").over(w2).alias("running_balance"),
    )
)
