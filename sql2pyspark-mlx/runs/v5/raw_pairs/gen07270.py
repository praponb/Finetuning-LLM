from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        F.lag("amount", 1).over(w).alias("prev_amount"),
        F.count("*").over(w2).alias("txn_type_count"),
    )
)
