from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_transactions
    .groupBy(F.year("txn_time").alias("txn_year"))
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("txn_year").desc())
)
