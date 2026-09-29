from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(76000, 90000))
    .groupBy("job_title")
    .pivot("level", ["L4", "L1"])
    .agg(F.count("salary"))
    .orderBy("job_title")
)
