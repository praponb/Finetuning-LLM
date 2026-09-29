from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("rating_tier").orderBy(F.col("row_count").desc())

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.4, "HIGH").otherwise("LOW").alias("rating_tier"),
        "city",
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 1)
    .withColumn("row_count_dense_rank", F.dense_rank().over(w))
    .orderBy("city")
)
