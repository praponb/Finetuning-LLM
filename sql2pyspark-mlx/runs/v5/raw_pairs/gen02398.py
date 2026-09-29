from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .pivot("level", ["L2", "L3"])
    .agg(F.max("salary"))
)
