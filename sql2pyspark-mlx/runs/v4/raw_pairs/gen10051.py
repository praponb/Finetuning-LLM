from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("cost")

result = (
    df_products
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "supplier_id", "cost", "brand")
    .orderBy(F.col("cost").desc())
)
