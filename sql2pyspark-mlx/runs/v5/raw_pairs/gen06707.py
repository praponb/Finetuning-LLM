from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")

latest = (
    df_suppliers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("rating") >= 3.6, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.min("rating").alias("min_rating"),
    )
    .orderBy("min_rating")
)
