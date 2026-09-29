from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc())

result = (
    df_shipments
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") == 1)
    .select("id", "status", "cost")
    .orderBy("status", F.col("cost").desc())
)
