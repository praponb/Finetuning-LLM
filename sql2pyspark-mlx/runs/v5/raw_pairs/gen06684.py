from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L1", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l1"),
        F.max("salary").alias("max_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .filter(F.col("D.max_salary") >= 92000)
    .select(
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("D.pct_l1"),
        F.col("D.max_salary"),
    )
    .orderBy(F.col("dept_name").desc(), F.col("max_salary").desc())
)
