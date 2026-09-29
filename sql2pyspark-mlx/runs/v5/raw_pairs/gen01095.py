from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")
w2 = Window.partitionBy("account_id")

result = (
    df_transactions
    .select(
        "id",
        "txn_time",
        "amount",
        "account_id",
        F.sum("amount").over(w).alias("running_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 3).alias("amount_share"),
    )
)
