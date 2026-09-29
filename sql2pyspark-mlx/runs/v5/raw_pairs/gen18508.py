from pyspark.sql import functions as F

recent_employees = df_employees.filter(F.col("email").isNull())

employees_stats = (
    recent_employees
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
)

result = (
    employees_stats
    .select("hire_month", "min_hire_date", "level_values")
    .orderBy(F.col("min_hire_date").desc(), "level_values")
)
