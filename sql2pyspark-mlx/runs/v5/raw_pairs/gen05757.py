from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age")

result = df_customers.select("id", "country", "age", F.rank().over(w).alias("rnk"))
