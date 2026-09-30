from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w3 = Window.orderBy("rating", "id")

result = (
    df_suppliers
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("rating_bucket", F.ntile(3).over(w2))
    .withColumn("rating_bucket_2", F.ntile(5).over(w3))
)
