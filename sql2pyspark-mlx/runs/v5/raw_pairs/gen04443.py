from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

result = (
    df_order_items
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 2)
)
