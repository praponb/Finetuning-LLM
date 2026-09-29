from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("manager_id", "dept_id")
    .pivot("job_title", ["Director", "Manager"])
    .agg(F.avg("salary"))
)
