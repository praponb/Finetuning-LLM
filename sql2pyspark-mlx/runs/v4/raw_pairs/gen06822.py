from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")

result = (
    df_stores
    .select("id", "region", "sqft", F.avg("sqft").over(w).alias("region_avg_sqft"))
)
