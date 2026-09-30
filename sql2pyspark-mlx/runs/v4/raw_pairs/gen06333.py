from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L3", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l3"),
        F.sum(F.when(F.col("job_title") == "Designer", 1).otherwise(0)).alias("designer_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.pct_l3") > 44)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("t.pct_l3"),
        F.col("t.designer_count"),
    )
    .orderBy(F.col("id").desc())
)
