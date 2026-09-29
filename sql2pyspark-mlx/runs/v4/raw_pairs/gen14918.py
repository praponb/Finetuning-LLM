from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "inner")
    .groupBy(
        F.col("departments.dept_name"),
        F.when(F.col("departments.location").isin(["Singapore", "New York"]), "priority").otherwise("other").alias("location_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("employees.level")))).alias("level_values"),
    )
    .orderBy("level_values")
)
