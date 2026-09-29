from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("dept_name").isNull())

agg = (
    recent_departments
    .groupBy("location")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.min("budget").alias("min_budget"),
    )
)

result = (
    agg
    .filter(F.col("min_budget") >= 1644000)
    .select("location", "location_values")
)
