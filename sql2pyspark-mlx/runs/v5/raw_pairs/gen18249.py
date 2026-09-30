from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time")
w2 = Window.partitionBy("account_id")

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "account_id",
        F.dense_rank().over(w).alias("txn_time_dense_rank"),
        F.count("*").over(w2).alias("account_id_count"),
    )
)
