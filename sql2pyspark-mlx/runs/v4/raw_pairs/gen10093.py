from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .withColumn("country_min_rating", F.min("rating").over(w))
    .withColumn("city_min_rating", F.min("rating").over(w2))
    .withColumn("rating_bucket", F.ntile(5).over(w3))
)
