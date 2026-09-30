from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 2)
)
