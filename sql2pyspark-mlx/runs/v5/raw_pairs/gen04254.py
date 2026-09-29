from pyspark.sql import functions as F

dim_totals = (
    df_employees
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "inner")
    .rollup(
        F.col("p.location").alias("location"),
        F.col("p.dept_name").alias("dept_name"),
        F.col("src.level").alias("level"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("src.salary")), 2).alias("total_salary"),
    )
)

result = dim_totals.select("location", "dept_name", "level", "rows_total", "total_salary")
