from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy("amount", "id")
w2 = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("txn_time", "id")
w_ordered = Window.partitionBy("account_id").orderBy("amount", "id")
w5 = Window.orderBy("amount", "id")

result = (
    df_transactions
    .withColumn("amount_bucket", F.ntile(5).over(w))
    .withColumn("last_known_amount", F.last("amount", ignorenulls=True).over(w2))
    .withColumn("prev_amount", F.lag("amount", 2).over(w_grp))
    .withColumn("amount_bucket_2", F.ntile(4).over(w_ordered))
    .withColumn("next_amount", F.lead("amount", 2).over(w5))
)
