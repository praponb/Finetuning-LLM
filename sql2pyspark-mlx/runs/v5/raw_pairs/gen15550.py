from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w_hist = Window.partitionBy("account_id").orderBy(F.col("amount").desc_nulls_last())
w5 = Window.partitionBy("txn_type", "merchant").orderBy("txn_time", "id")

with_windows = (
    df_transactions
    .select(
        "id",
        "account_id",
        "merchant",
        "is_flagged",
        F.avg("amount").over(w).alias("amount_avg_5"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
        F.row_number().over(w3).alias("event_seq"),
        F.dense_rank().over(w_hist).alias("amount_dense_rank"),
        F.lag("amount", 1).over(w5).alias("prev_amount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "merchant",
        "amount_avg_5",
        "last_known_amount",
        "amount_dense_rank",
        "prev_amount",
        F.when(F.col("merchant").rlike("^(Shell|Amazon)$"), "EXTENDED").otherwise("GROUP_B").alias("merchant_zone"),
    )
    .orderBy(F.col("id").desc(), "last_known_amount")
    .limit(10)
)
