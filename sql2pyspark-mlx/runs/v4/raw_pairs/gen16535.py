from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = df_customers.select("id", "city", F.count("*").over(w).alias("city_count"))
