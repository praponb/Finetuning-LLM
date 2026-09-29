from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_rating").desc())

result = (
    df_suppliers
    .groupBy("city")
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("rating_rank", F.rank().over(w))
    .orderBy("total_rating", "city")
    .limit(1)
)
