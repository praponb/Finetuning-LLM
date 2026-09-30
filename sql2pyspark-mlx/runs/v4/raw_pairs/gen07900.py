from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.countDistinct("country").alias("distinct_countries"),
        F.avg("rating").alias("avg_rating_2"),
    )
    .orderBy(F.col("distinct_countries").asc_nulls_first())
)
