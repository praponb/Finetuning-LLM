from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_stores
    .withColumn("last_known_sqft", F.last("sqft", ignorenulls=True).over(w))
    .withColumn("sqft_avg_2", F.avg("sqft").over(w2))
)
