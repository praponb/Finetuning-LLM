from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_month")

result = (
    df_transactions
    .filter(F.col("txn_type") != "FEE")
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
)
