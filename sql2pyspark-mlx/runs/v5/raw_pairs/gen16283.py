from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("txn_time") >= "2025-06-01")
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        F.avg("amount").over(w).alias("amount_avg_5_prior"),
        F.lag("amount", 1).over(w2).alias("prev_amount"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
