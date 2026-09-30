from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type").orderBy("balance", "id")

result = (
    df_accounts
    .withColumn("running_balance", F.sum("balance").over(w))
    .withColumn("balance_bucket", F.ntile(5).over(w2))
)
