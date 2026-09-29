from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_type")

result = (
    df_transactions
    .groupBy("txn_type")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy(F.col("total_amount").desc())
)
