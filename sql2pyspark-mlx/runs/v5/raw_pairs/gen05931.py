from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .filter(F.hour("txn_time").between(10, 18))
    .select(
        "*",
        F.count("amount").over(w).alias("last4_count_amount"),
        F.avg("amount").over(w).alias("last4_avg_amount"),
        F.min("amount").over(w).alias("last4_min_amount"),
        F.sum("amount").over(w2).alias("running_amount"),
    )
)
