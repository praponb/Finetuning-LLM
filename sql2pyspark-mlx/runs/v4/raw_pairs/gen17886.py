from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_suppliers
    .filter(F.col("rating") <= 1.9)
    .select("id", "city", "rating", F.sum("rating").over(w).alias("city_sum_rating"))
)
