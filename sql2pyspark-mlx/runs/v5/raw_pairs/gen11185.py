from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_sqft").desc())

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.count("*").alias("cnt"),
    )
    .withColumn("rnk", F.rank().over(w))
    .orderBy("rnk")
)
