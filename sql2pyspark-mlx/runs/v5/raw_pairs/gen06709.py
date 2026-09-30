from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "merchant",
        "amount",
        "txn_time",
        F.ntile(2).over(w).alias("amount_ntile"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)
