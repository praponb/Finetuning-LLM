from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .pivot("level", ["L5", "L4", "L1", "L3"])
    .agg(F.max("salary"))
)
