from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.sum("sqft").alias("total_sqft"),
    )
    .orderBy("region")
)
