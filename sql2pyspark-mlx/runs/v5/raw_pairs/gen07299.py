from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.max("sqft").alias("max_sqft"),
    )
    .filter(F.col("max_sqft") < 11200)
    .orderBy(F.col("region_values").desc())
)
