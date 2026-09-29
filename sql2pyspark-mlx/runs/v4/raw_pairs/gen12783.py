from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("country")
w3 = Window.orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w5 = Window.partitionBy("city").orderBy(F.col("rating").desc_nulls_first())

result = (
    df_suppliers
    .withColumn("rating_bucket", F.ntile(4).over(w))
    .withColumn("country_sum_rating", F.sum("rating").over(w2))
    .withColumn("rating_avg_6", F.avg("rating").over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("rating_cume_dist", F.cume_dist().over(w5))
)
