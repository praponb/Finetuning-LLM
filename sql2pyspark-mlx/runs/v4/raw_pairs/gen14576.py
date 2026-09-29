from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy("sqft", "id")
w3 = Window.partitionBy("region").orderBy("sqft", "id")
w4 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
    .withColumn("sqft_share_2", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
    .withColumn("sqft_bucket", F.ntile(4).over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w4))
)
