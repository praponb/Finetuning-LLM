from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())
w2 = Window.partitionBy("merchant", "account_id").orderBy("txn_time")

result = (
    df_transactions
    .filter(F.col("amount") > 3000)
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "account_id",
        "txn_time",
        F.dense_rank().over(w).alias("amount_dense_rank"),
        F.dense_rank().over(w2).alias("txn_time_dense_rank"),
    )
)
