from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating_bucket")

result = (
    df_suppliers
    .filter(F.length("supplier_name") >= 14)
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_rating", F.sum("total_rating").over(w))
    .orderBy(F.col("cumulative_rating").desc(), "rating_bucket")
    .limit(3)
)
