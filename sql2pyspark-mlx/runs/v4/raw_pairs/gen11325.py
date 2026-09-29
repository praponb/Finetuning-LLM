from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "region", "sqft", "opened_date")
)
