from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id")
w2 = Window.orderBy("txn_time", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "txn_time",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        F.avg("amount").over(w2).alias("amount_avg_5"),
        F.last("merchant", ignorenulls=True).over(w3).alias("last_known_merchant"),
    )
)
