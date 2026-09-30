from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").desc())

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 3)
    .orderBy("id")
)
