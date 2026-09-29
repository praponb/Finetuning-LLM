from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "txn_time",
        F.row_number().over(w).alias("row_num"),
        F.lag("amount", 1).over(w2).alias("prev_amount"),
    )
    .orderBy("txn_type")
)
