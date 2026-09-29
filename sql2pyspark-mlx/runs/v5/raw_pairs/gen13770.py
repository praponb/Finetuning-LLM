from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc())

result = (
    df_products
    .filter(F.col("supplier_id") == 1)
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "supplier_id", "cost")
)
