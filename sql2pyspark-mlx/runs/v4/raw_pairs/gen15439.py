from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("amount").desc(), "id")

result = (
    df_orders
    .filter(F.col("shipping_fee") >= 39)
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .orderBy(F.col("amount").desc())
)
