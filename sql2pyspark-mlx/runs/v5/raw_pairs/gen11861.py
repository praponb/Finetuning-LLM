from pyspark.sql import functions as F

result = (
    df_employees
    .alias("e")
    .join(df_departments.alias("p"), F.col("e.dept_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.dept_name").alias("dept_name"),
        F.col("p.location").alias("location"),
        F.col("e.level").alias("level"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("e.salary")).alias("avg_salary"),
    )
    .orderBy("avg_salary")
)
