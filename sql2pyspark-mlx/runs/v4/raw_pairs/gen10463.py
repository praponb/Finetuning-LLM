from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_first())
w3 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc_nulls_first())
w4 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_accounts
    .withColumn("balance_avg_3", F.avg("balance").over(w))
    .withColumn("balance_percent_rank", F.percent_rank().over(w2))
    .withColumn("balance_cume_dist", F.cume_dist().over(w3))
    .withColumn("cumulative_balance", F.sum("balance").over(w4))
)
