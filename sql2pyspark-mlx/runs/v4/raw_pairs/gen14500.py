from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant")

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "txn_time",
        "amount",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        F.min("amount").over(w2).alias("merchant_min_amount"),
    )
    .orderBy("amount", F.col("prev_amount").desc())
)
