from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("end_date")

result = df_subscriptions.select("id", "end_date", F.rank().over(w).alias("rnk"))
