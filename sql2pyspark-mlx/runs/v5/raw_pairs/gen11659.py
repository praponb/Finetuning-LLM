from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") <= "2024-06-13")
    .groupBy("manager_id")
    .pivot("job_title", ["Engineer", "Designer", "Director"])
    .agg(F.avg("salary"))
)
