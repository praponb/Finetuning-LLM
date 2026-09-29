from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .filter(F.col("merchant").isNull())
    .select(
        "id",
        "txn_time",
        "amount",
        "account_id",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_4"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)
