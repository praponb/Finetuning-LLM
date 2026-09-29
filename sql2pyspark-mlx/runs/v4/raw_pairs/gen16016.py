from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("discount").desc())

result = (
    df_orders
    .filter(F.col("status") == "SHIPPED")
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 5)
    .select("id", "status", "discount", "amount")
    .orderBy(F.col("status").desc())
)
