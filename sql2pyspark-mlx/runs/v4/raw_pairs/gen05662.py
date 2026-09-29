from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id")
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .withColumn("rating_bucket", F.ntile(10).over(w))
    .withColumn("cumulative_rating", F.sum("rating").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)
