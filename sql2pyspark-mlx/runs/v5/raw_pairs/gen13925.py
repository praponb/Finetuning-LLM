from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .filter(F.col("amount").between(2000, 3000))
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        F.lag("amount", 2).over(w).alias("prev_amount"),
    )
)
