from pyspark.sql import functions as F

result = (
    df_employees
    .alias("a")
    .join(df_departments.alias("dim"), F.col("a.dept_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.dept_name").alias("dept_name"), F.col("a.level").alias("level"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.salary")), 2).alias("total_salary"),
    )
)
