from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc())

result = (
    df_inventory
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "product_id", "last_restock", "quantity")
)
