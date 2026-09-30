from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("rating", "id")
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .withColumn("running_rating", F.sum("rating").over(w))
    .withColumn("rating_bucket", F.ntile(10).over(w2))
    .withColumn("last_known_city", F.last("city", ignorenulls=True).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("rating_bucket_2", F.ntile(4).over(w4))
)
