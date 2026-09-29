from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "txn_type",
        "txn_time",
        "amount",
        F.lead("amount", 1).over(w).alias("next_amount"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
    .orderBy("running_amount")
)
