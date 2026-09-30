from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc_nulls_first())
w2 = Window.partitionBy("city", "region")

result = (
    df_stores
    .withColumn("sqft_rank", F.rank().over(w))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4))
)
