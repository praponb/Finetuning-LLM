from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("balance", "id")
w_running = Window.partitionBy("status").orderBy(F.col("balance_filled").desc())
w3 = Window.partitionBy("account_type", "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("balance").desc(), "id")
w5 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_accounts
    .filter(F.col("status").isNotNull() & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

result = (
    filtered
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "balance_filled",
        F.lead("balance_filled", 1).over(w).alias("next_balance_filled"),
        F.rank().over(w_running).alias("balance_filled_rank"),
        F.sum("balance_filled").over(w3).alias("running_balance_filled"),
        F.row_number().over(w4).alias("row_seq"),
        F.last("status", ignorenulls=True).over(w5).alias("last_known_status"),
    )
)
