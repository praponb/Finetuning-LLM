from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc(), "id")

result = (
    df_orders
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 5)
)
