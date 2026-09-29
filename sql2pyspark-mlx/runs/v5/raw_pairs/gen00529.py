from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit")

result = df_customers.select("id", "credit_limit", F.dense_rank().over(w).alias("dense_rnk"))
