from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.stddev_samp("rating").alias("rating_stddev"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("country").desc_nulls_first())
)
