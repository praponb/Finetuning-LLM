from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("paid_quarter")

result = (
    df_payments
    .groupBy(F.quarter("paid_at").alias("paid_quarter"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
)
