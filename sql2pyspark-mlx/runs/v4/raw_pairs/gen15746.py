from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") == 1)
    .orderBy(F.col("pos").desc())
)
