from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")

result = df_stores.select("id", "city", "sqft", F.ntile(4).over(w).alias("bucket"))
