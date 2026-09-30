from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("sqft", "id")

result = (
    df_stores
    .filter(F.col("city") == "Singapore")
    .select("id", "city", "region", "sqft", F.ntile(3).over(w).alias("bucket"))
)
