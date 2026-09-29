from pyspark.sql import functions as F

src = df_employees.filter(F.col("level").isin(["L2", "L3"]))

hire_quarter_summary = (
    src
    .groupBy(F.quarter("hire_date").alias("hire_quarter"), "job_title")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
)

result = (
    hire_quarter_summary
    .filter(F.col("salary_range") < 53)
    .select("hire_quarter", "job_title", "salary_range")
    .orderBy(F.col("hire_quarter").desc(), "job_title")
    .limit(50)
)
