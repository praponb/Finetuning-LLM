from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "right")
    .filter(F.col("d.budget").between(1167000, 1763000) & F.col("e.is_remote"))
    .select(
        F.col("e.id"),
        F.col("e.salary"),
        F.col("d.location"),
        F.col("d.dept_name"),
        F.col("d.budget"),
    )
    .orderBy(F.col("budget").desc(), F.col("salary").desc())
)
