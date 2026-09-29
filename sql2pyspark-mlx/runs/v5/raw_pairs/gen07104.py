from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").asc_nulls_last())
w3 = Window.partitionBy("customer_id", "status").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("status") != "FROZEN")
    .withColumn("running_balance", F.sum("balance").over(w))
    .withColumn("balance_percent_rank", F.percent_rank().over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)
