from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
        F.min("budget").alias("min_budget"),
        F.count("*").alias("department_count"),
    )
)
