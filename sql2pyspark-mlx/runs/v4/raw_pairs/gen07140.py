from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("account_type", "status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .withColumn("status_stddev_samp_balance", F.stddev_samp("balance").over(w))
    .withColumn("last_known_status", F.last("status", ignorenulls=True).over(w2))
    .withColumn("running_balance", F.sum("balance").over(w3))
)
