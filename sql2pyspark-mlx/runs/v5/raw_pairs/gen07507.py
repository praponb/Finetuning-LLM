from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_transactions
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("rnk", F.rank().over(w))
)
