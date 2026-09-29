from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_first())
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_suppliers
    .withColumn("rating_cume_dist", F.cume_dist().over(w))
    .withColumn("last_known_city", F.last("city", ignorenulls=True).over(w2))
)
