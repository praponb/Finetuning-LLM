from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("balance", "id")
w3 = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id")

result = (
    df_accounts
    .withColumn("running_balance", F.sum("balance").over(w))
    .withColumn("balance_bucket", F.ntile(5).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)
