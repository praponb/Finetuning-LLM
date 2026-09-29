from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("txn_type", "merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .withColumn("running_amount", F.sum("amount").over(w))
    .withColumn("cumulative_amount", F.sum("amount").over(w2))
)
