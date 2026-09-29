from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant", "txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        "merchant",
        F.sum("amount").over(w).alias("running_amount"),
        F.lag("amount", 1).over(w2).alias("prev_amount"),
    )
)
