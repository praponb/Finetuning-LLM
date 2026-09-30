from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .filter(F.col("sqft") < 7525)
    .withColumn("prev_sqft", F.lag("sqft", 1).over(w))
    .withColumn("region_min_sqft", F.min("sqft").over(w2))
    .withColumn("sqft_avg_2_prior", F.avg("sqft").over(w3))
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w4))
)
