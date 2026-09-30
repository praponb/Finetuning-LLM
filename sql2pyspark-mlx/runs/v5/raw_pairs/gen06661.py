from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("location").isin(["Bangkok", "London"]))

location_summary = (
    valid_departments
    .groupBy("location")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("dept_name"))).alias("dept_name_values"),
    )
)

result = location_summary.select("location", "dept_name_values")
