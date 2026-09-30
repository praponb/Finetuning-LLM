from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .filter(F.col("opened_date") < "2023-12-28")
    .withColumn("rnk", F.row_number().over(w))
    .filter(F.col("rnk") <= 5)
)
