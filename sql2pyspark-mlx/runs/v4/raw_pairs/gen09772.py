from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("shipping_fee").desc())

result = (
    df_orders
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 1)
)
