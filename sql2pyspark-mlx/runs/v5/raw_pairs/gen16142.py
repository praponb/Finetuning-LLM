from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("salary") >= 89000)
    .groupBy("dept_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("d.location"),
        F.col("ds.row_count"),
    )
)
