from pyspark.sql import functions as F

result = (
    df_employees
    .alias("s")
    .join(df_departments.alias("dim"), F.col("s.dept_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["dept_name", "location"], ["dept_name"]],
        F.col("dim.dept_name").alias("dept_name"),
        F.col("dim.location").alias("location"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("s.salary")), 2).alias("avg_salary"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("dept_name").asc_nulls_first())
)
