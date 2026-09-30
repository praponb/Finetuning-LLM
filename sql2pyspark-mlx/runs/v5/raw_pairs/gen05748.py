from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(-3, -1)
w_part = Window.partitionBy("country")
w3 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_suppliers
    .withColumn("rating_avg_3_prior", F.avg("rating").over(w))
    .withColumn("country_max_rating", F.max("rating").over(w_part))
    .withColumn("cumulative_rating", F.sum("rating").over(w3))
)
