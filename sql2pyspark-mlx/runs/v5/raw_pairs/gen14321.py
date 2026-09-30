from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id")

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "amount",
        "txn_time",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "merchant", "amount", "txn_time")
    .orderBy("txn_time")
)
