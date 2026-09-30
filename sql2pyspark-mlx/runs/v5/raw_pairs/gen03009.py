from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title").isin(["Analyst", "Director"]) | (F.col("level") == "L5"))
    .groupBy("level", F.quarter("hire_date").alias("hire_quarter"))
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
    .filter(F.col("salary_range") >= 47)
)
