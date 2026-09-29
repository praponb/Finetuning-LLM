from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("sqft"))).alias("bangkok_total_sqft"),
    )
    .orderBy(F.col("city").asc_nulls_first())
)
