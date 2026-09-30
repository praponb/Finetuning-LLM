from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount")

result = (
    df_payments
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 3)
    .select("id", "status", "amount", "order_id")
    .orderBy(F.col("amount").desc())
)
