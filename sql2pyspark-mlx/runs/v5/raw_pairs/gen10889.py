from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc())

result = df_shipments.select("id", "status", "cost", F.rank().over(w).alias("rnk"))
