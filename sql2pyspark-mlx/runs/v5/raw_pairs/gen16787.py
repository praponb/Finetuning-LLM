from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_month")

result = (
    df_transactions
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy(F.col("total_amount").desc(), "txn_month")
    .limit(5)
)
