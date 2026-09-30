from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant")

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.row_number().over(w_recent).alias("row_seq"),
        F.avg("amount").over(w2).alias("merchant_avg_amount"),
    )
)
