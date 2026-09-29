from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.hour("txn_time").between(8, 12))
    .select(
        "id",
        "merchant",
        "txn_time",
        "amount",
        F.lag("amount", 2).over(w).alias("prev_amount"),
        F.row_number().over(w2).alias("amount_row"),
    )
    .orderBy(F.col("txn_time").desc(), F.col("amount").desc())
)
