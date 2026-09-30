from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.location") == "New York")
    .groupBy(F.col("d.location"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.round(((100 * F.sum(F.when(F.col("e.level") == "L2", 1).otherwise(0))) / F.count("*")), 1).alias("pct_l2"),
    )
    .orderBy("location")
    .limit(50)
)
