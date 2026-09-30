from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_suppliers
    .select("id", "city", "rating", F.avg("rating").over(w).alias("city_avg_rating"))
    .orderBy(F.col("city_avg_rating").desc())
)
