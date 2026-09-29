from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")

result = df_stores.select("id", "city", "sqft", F.ntile(10).over(w).alias("quartile"))
