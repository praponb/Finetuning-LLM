from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("city")

result = (
    df_suppliers
    .filter(F.col("country") != "DE")
    .groupBy("city")
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("cumulative_rating", F.sum("total_rating").over(w))
    .orderBy(F.col("total_rating").desc(), F.col("cumulative_rating").desc(), "city")
    .limit(5)
)
