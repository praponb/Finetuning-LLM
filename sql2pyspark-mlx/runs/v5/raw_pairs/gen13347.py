from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("job_title")
    .pivot("level", ["L3", "L2", "L1", "L4"])
    .agg(F.max("salary"))
    .orderBy("job_title")
)
