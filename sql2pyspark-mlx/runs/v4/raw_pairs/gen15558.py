from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc())

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "order_id", "amount", "method")
    .orderBy(F.col("id").desc())
)
