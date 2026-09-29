from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_stores
    .filter(F.col("city") == "Bangkok")
    .select("id", "city", "sqft", F.max("sqft").over(w).alias("city_max_sqft"))
    .orderBy("city_max_sqft")
)
