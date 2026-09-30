from pyspark.sql import functions as F

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.hire_date") < "2024-09-01"), "left_anti")
)

result = (
    kept
    .cube("dept_name", "location")
    .agg(
        F.count_if(F.col("location") == "Singapore").alias("singapore_events"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("dept_name").asc_nulls_first())
)
