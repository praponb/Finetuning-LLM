from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city").orderBy("opened_date", "id")

result = (
    df_stores
    .withColumn("sqft_bucket", F.ntile(3).over(w))
    .withColumn("next_sqft", F.lead("sqft", 1).over(w2))
)
