from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id")
w2 = Window.partitionBy("txn_type", "merchant").orderBy("amount", "id")
w3 = Window.partitionBy("account_id", "merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_transactions
    .withColumn("next_amount", F.lead("amount", 2).over(w))
    .withColumn("prev_amount", F.lag("amount", 1).over(w2))
    .withColumn("first_known_merchant", F.first("merchant", ignorenulls=True).over(w3))
)
