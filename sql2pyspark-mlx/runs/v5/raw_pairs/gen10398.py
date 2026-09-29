from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_transactions
    .filter(F.col("txn_time") < "2024-12-21")
    .select(
        "id",
        "account_id",
        "amount",
        "txn_type",
        "txn_time",
        F.rank().over(w).alias("rnk"),
        F.round(F.avg("amount").over(w2), 2).alias("amount_moving_avg_4"),
    )
)
