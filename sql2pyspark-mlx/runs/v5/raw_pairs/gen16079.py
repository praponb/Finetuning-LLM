from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 9, 10]))
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "product_id", "quantity", "store_id")
)
