from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc())

result = (
    df_payments
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 2)
    .orderBy(F.col("amount").desc())
)
