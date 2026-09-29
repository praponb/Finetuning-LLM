from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("balance").asc_nulls_last())
w3 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .withColumn("first_known_balance", F.first("balance", ignorenulls=True).over(w))
    .withColumn("balance_cume_dist", F.cume_dist().over(w2))
    .withColumn("last_known_balance", F.last("balance", ignorenulls=True).over(w3))
)
