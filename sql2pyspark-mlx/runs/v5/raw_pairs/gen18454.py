from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date").desc())

result = (
    df_orders
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)
