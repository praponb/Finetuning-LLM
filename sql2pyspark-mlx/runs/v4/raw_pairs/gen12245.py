from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        F.rank().over(w).alias("rnk"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_4"),
    )
)
