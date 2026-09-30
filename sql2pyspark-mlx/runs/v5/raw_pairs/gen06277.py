from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())

result = (
    df_order_items
    .filter(F.col("unit_price") > 200)
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .orderBy(F.col("id").desc())
)
