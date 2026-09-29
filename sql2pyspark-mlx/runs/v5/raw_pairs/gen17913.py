from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_rating").desc())

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.dense_rank().over(w))
)
