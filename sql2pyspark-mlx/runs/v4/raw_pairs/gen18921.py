from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type")

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "txn_type",
        "txn_time",
        "amount",
        F.sum("amount").over(w).alias("running_amount"),
        F.max("amount").over(w2).alias("txn_type_max_amount"),
    )
)
