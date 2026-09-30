from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .withColumn("status_max_balance", F.max("balance").over(w))
    .withColumn("customer_id_sum_balance", F.sum("balance").over(w2))
    .withColumn("first_known_balance", F.first("balance", ignorenulls=True).over(w3))
)
