from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region")

metrics = (
    df_stores
    .withColumn("city_avg_sqft", F.avg("sqft").over(w))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w2))
    .withColumn("region_sum_sqft", F.sum("sqft").over(w3))
)

result = (
    metrics
    .select("id", "store_name", "region", "opened_date")
    .orderBy(F.col("opened_date").desc())
)
