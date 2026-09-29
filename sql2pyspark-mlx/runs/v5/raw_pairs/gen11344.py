from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("account_id")

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "merchant",
        "txn_time",
        "amount",
        "account_id",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_3"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 3).alias("amount_share"),
    )
)
