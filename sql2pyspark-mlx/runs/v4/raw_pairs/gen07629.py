from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc())
w2 = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy("account_type", "status").orderBy("balance", "id")

with_windows = (
    df_accounts
    .select(
        "*",
        F.dense_rank().over(w).alias("balance_dense_rank"),
        F.row_number().over(w2).alias("row_seq"),
        F.lag("balance", 1).over(w3).alias("prev_balance"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.sum(F.when(F.col("account_type") == "LOAN", F.col("balance_dense_rank")).otherwise(0)).alias("loan_balance_dense_rank"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_rows"),
        F.min("balance").alias("min_balance"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
    )
    .orderBy(F.col("loan_balance_dense_rank").asc_nulls_last())
)
