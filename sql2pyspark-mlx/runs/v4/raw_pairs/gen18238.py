from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc(), "id")
w2 = Window.orderBy(F.col("sqft").desc_nulls_first())

result = (
    df_stores
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("sqft_cume_dist", F.cume_dist().over(w2))
)
