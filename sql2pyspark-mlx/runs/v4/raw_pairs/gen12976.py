from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id")
w_ordered = Window.partitionBy("txn_type").orderBy("amount", "id")
w_ordered_2 = Window.orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .withColumn("prev_amount", F.lag("amount", 1).over(w))
    .withColumn("amount_bucket", F.ntile(4).over(w_ordered))
    .withColumn("event_seq", F.row_number().over(w_ordered_2))
)
