from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .pivot("level", ["L5", "L2"])
    .agg(F.sum("salary"))
)
