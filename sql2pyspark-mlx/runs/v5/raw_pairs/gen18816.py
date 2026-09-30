from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time")
w2 = Window.partitionBy("txn_type")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "txn_type",
        "amount",
        F.rank().over(w).alias("rnk"),
        F.max("amount").over(w2).alias("txn_type_max_amount"),
    )
)
