from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("status").orderBy("balance_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status", "account_type").orderBy("balance", "id")

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-04-11").cast("date")) & (F.col("customer_id") == 18))
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.floor("balance").alias("balance_whole"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_clean"),
    )
)

result = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.sum("balance_whole").over(w2).alias("status_sum_balance_whole"),
        F.sum("balance").over(w3).alias("cumulative_balance"),
        F.lag("balance_whole", 2, 0).over(w4).alias("prev_balance_whole"),
    )
)
