from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "London")
    .groupBy("location", "dept_name")
    .agg(
        F.max("budget").alias("max_budget"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)
