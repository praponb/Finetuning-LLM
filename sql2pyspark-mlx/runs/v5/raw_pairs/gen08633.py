from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc())

result = (
    df_stores
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 3)
    .orderBy("region")
)
