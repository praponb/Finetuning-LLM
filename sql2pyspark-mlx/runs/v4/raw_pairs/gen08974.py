from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount")
w2 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .filter(F.col("account_id").isin([13, 24, 25]))
    .select(
        "id",
        "merchant",
        "amount",
        "txn_type",
        "txn_time",
        F.dense_rank().over(w).alias("amount_dense_rank"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("txn_time").desc())
)
