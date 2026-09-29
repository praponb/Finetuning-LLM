from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc_nulls_first())
w2 = Window.partitionBy("account_type")
w3 = Window.partitionBy("status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id", "account_type").orderBy(F.col("balance").desc_nulls_last())

result = (
    df_accounts
    .withColumn("balance_rank", F.rank().over(w))
    .withColumn("account_type_stddev_samp_balance", F.stddev_samp("balance").over(w2))
    .withColumn("cumulative_balance", F.sum("balance").over(w3))
    .withColumn("balance_cume_dist", F.cume_dist().over(w4))
)
