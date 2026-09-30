from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age")

result = df_customers.select("id", "city", "age", F.rank().over(w).alias("rnk"))
