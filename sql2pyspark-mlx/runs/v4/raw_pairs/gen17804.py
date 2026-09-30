from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "txn_time",
        "amount",
        "account_id",
        F.round(F.avg("amount").over(w), 2).alias("amount_moving_avg_4"),
        F.dense_rank().over(w2).alias("amount_dense_rank"),
    )
)
