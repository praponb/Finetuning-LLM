from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc(), "id")

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "txn_time",
        F.ntile(10).over(w).alias("bucket"),
        F.row_number().over(w2).alias("rn"),
    )
)
