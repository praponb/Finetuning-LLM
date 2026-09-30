from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("amount", "id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "txn_time",
        "amount",
        "txn_type",
        F.lag("amount", 2).over(w).alias("prev_amount"),
        F.row_number().over(w2).alias("amount_row"),
    )
    .orderBy(F.col("txn_time").desc())
)
