from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc())

result = (
    df_stores
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 3)
    .orderBy("city")
)
