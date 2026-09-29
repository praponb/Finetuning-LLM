from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget")

result = df_departments.select("id", "budget", F.rank().over(w).alias("rnk"))
