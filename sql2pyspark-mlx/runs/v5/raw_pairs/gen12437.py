from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 1)
)
