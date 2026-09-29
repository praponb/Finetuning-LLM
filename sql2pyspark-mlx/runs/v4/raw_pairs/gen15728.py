from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("job_title") != "Engineer")
    .groupBy("job_title")
    .pivot("level", ["L3", "L5", "L1"])
    .agg(F.max("salary"))
    .orderBy("job_title")
)
