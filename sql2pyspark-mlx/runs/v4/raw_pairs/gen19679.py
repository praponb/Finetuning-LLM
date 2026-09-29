from pyspark.sql import functions as F

recent_employees = df_employees.filter(F.col("level") == "L4")

summary = (
    recent_employees
    .groupBy("dept_id")
    .agg(
        F.count("*").alias("total_count"),
        F.max("hire_date").alias("last_hire_date"),
    )
)

result = (
    summary
    .filter(F.col("total_count") < 6)
    .orderBy(F.col("dept_id").desc())
    .limit(10)
)
