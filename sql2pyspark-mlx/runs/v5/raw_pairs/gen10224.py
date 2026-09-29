from pyspark.sql import functions as F

result = (
    df_employees
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .rollup(F.col("d.dept_name").alias("dept_name"), F.col("e.level").alias("level"))
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("e.salary")).alias("avg_salary"),
    )
    .orderBy(F.col("level").asc_nulls_last())
)
