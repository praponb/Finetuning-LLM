from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("amount").desc(), "id")
w2 = Window.orderBy("txn_time", "id")
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.ntile(5).over(w).alias("amount_bucket"),
        F.lag("amount", 2).over(w2).alias("prev_amount"),
        F.avg("amount").over(w3).alias("amount_avg_5"),
    )
)
