from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price")

latest = (
    df_order_items
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select("id", "order_id", "product_id", "quantity", "row_num")
    .orderBy(F.col("row_num").desc())
)
