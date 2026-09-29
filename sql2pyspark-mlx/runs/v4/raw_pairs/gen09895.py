from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.col("amount") < 1700)
    .select(
        "id",
        "merchant",
        "amount",
        "txn_type",
        F.first("txn_type").over(w).alias("first_txn_type"),
    )
)
