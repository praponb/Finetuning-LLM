from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(~F.col("location").isin(["New York", "London"]))
    .withColumn("location_length", F.length("location"))
    .withColumn("location_dept_name", F.concat("location", F.lit("/"), "dept_name"))
    .withColumn("location_filled", F.coalesce("location", F.lit("N/A")))
)

result = (
    cleaned
    .groupBy("dept_name")
    .agg(
        F.percentile_approx("location_length", 0.5).alias("location_length_p50"),
        F.count_if(F.col("location") == "Berlin").alias("berlin_events"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.max("location_length").alias("max_location_length"),
    )
)
