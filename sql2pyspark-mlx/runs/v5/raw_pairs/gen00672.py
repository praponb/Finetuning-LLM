from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")

result = (
    df_inventory
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "product_id", "quantity", "store_id")
)
