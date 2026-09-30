from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("method")

result = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .groupBy("method")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_amount", F.sum("total_amount").over(w))
    .orderBy(F.col("total_amount").desc(), "method")
    .limit(10)
)
