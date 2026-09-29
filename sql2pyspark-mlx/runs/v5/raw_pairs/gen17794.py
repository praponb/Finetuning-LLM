from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("job_title") != "Analyst")
    .groupBy("dept_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("level") == "L4", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l4"),
        F.round(((100 * F.sum(F.when(F.col("level") == "L5", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l5"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "left")
    .filter(F.col("dt.pct_l5") > 58)
    .select(F.col("d.id"), F.col("d.budget"), F.col("dt.pct_l4"), F.col("dt.pct_l5"))
    .orderBy(F.col("pct_l4").desc())
)
