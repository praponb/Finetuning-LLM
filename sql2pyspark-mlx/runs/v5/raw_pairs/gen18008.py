from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.avg(F.when(F.col("country") == "SG", F.col("rating"))).alias("sg_avg_rating"),
    )
    .filter(F.col("rating_stddev") > 0)
    .orderBy(F.col("rating_stddev").desc_nulls_last())
)
