from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").asc_nulls_first())

result = (
    df_order_items
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)
