from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")

result = (
    df_stores
    .filter(F.month("opened_date").isin([2, 3, 6]))
    .select("id", "region", "sqft", F.max("sqft").over(w).alias("region_max_sqft"))
    .orderBy(F.col("id").desc())
)
