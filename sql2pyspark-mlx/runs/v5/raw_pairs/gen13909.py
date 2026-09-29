from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.salary").between(87000, 127000))
    .groupBy(F.col("d.location"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.min(F.col("e.salary")).alias("min_salary"),
        F.round(((100 * F.sum(F.when(F.col("e.level") == "L1", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l1"),
    )
)
