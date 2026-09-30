from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("launch_date").desc())

result = df_products.select("id", "launch_date", F.rank().over(w).alias("rnk"))
