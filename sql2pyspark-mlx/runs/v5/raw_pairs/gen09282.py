from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_amount").desc())

result = (
    df_payments
    .filter(F.col("order_id").isin([12, 39]))
    .groupBy("status")
    .agg(F.sum("amount").alias("total_amount"))
    .withColumn("amount_rank", F.dense_rank().over(w))
)
