from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("dept_id")
    .pivot("job_title", ["Designer", "Manager", "Engineer"])
    .agg(F.sum("salary"))
)
