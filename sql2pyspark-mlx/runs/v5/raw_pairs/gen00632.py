from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("customer_id")

result = (
    df_accounts
    .withColumn("last_known_status", F.last("status", ignorenulls=True).over(w))
    .withColumn("customer_id_min_balance", F.min("balance").over(w_ordered))
)
