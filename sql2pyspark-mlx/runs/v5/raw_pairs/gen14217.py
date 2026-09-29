from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id")

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        F.max("amount").over(w).alias("txn_type_max_amount"),
        F.lead("amount", 2).over(w2).alias("next_amount"),
    )
    .orderBy(F.col("txn_type").desc())
)
