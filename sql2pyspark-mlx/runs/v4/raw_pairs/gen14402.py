from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("customer_id")
w3 = Window.partitionBy("status").orderBy("opened_date", "id")

result = (
    df_accounts
    .withColumn("running_balance", F.sum("balance").over(w))
    .withColumn("customer_id_avg_balance", F.avg("balance").over(w_running))
    .withColumn("event_seq", F.row_number().over(w3))
)
