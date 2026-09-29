from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())

result = (
    df_stores
    .filter(F.col("store_name").like("Ol%"))
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 3)
)
