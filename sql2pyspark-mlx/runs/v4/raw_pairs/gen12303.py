from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft", "id")

result = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select("id", "sqft", F.ntile(10).over(w).alias("quartile"))
    .orderBy(F.col("quartile").desc())
)
