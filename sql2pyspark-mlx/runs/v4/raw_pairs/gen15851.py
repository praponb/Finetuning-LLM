from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_rating").desc())

result = (
    df_suppliers
    .groupBy("country")
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("rating_rank", F.rank().over(w))
    .orderBy("country", "total_rating")
)
