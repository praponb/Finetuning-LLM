from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.orderBy("opened_date", "id")
w3 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .withColumn("sqft_avg_3", F.avg("sqft").over(w))
    .withColumn("next_sqft", F.lead("sqft", 1).over(w2))
    .withColumn("sqft_bucket", F.ntile(5).over(w3))
)
