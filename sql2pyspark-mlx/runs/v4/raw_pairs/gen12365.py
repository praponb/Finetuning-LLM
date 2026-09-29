from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id", "merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

result = (
    df_transactions
    .withColumn("running_amount", F.sum("amount").over(w))
    .withColumn("amount_max_7d", F.max("amount").over(w2))
)
