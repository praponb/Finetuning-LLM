from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

latest = (
    df_suppliers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy(
        F.when(F.col("rating") >= 3.6, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.countDistinct("city").alias("distinct_cities"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_count"),
    )
)
