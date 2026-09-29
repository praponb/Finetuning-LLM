from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "txn_time",
        "amount",
        "txn_type",
        "merchant",
        F.sum("amount").over(w).alias("running_amount"),
        F.first("merchant").over(w2).alias("first_merchant"),
    )
)
