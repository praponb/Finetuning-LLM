from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status", "account_type").orderBy("balance", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_accounts
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w))
    .withColumn("balance_avg_5", F.avg("balance").over(w2))
)
