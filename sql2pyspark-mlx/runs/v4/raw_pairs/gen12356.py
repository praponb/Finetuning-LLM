from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-6, -1)
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-2, -1)

result = (
    df_suppliers
    .withColumn("rating_avg_6_prior", F.avg("rating").over(w))
    .withColumn("last_known_rating", F.last("rating", ignorenulls=True).over(w2))
    .withColumn("rating_avg_2_prior", F.avg("rating").over(w3))
)
