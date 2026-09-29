from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id")
w2 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .withColumn("prev_amount", F.lag("amount", 2).over(w))
    .withColumn("cumulative_amount", F.sum("amount").over(w2))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w3))
)
