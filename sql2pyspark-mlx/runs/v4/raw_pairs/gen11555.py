from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("account_type", "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("balance", "id")
w_recent = Window.partitionBy("account_type")

result = (
    df_accounts
    .withColumn("balance_bucket", F.ntile(4).over(w))
    .withColumn("last_known_balance", F.last("balance", ignorenulls=True).over(w2))
    .withColumn("running_balance", F.sum("balance").over(w3))
    .withColumn("next_balance", F.lead("balance", 2, 0).over(w4))
    .withColumn("account_type_avg_balance", F.avg("balance").over(w_recent))
)
