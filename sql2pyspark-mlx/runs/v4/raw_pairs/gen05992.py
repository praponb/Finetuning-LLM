from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy(F.col("amount").desc())

result = (
    df_transactions
    .withColumn("first_known_amount", F.first("amount", ignorenulls=True).over(w))
    .withColumn("amount_percent_rank", F.percent_rank().over(w2))
)
