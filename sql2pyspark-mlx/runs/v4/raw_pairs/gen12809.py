from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount")

result = (
    df_payments
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 2)
)
