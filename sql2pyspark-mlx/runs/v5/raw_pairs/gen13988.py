from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price")

result = (
    df_order_items
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 3)
    .orderBy(F.col("id").desc())
)
