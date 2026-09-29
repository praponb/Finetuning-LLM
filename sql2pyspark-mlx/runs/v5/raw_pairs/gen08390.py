from pyspark.sql import functions as F

src = (
    df_departments
    .filter(F.col("location") == "Bangkok")
    .withColumn("dept_name_location_key", F.concat_ws("-", "dept_name", "location"))
)

location_summary = (
    src
    .groupBy("location")
    .agg(F.sort_array(F.collect_set("location")).alias("location_list"))
)

result = (
    location_summary
    .select("location", "location_list")
    .orderBy(F.col("location").desc())
)
