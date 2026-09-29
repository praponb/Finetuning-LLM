from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("level").isin(["L4", "L1"]))
    .groupBy("job_title")
    .pivot("level", ["L5", "L2"])
    .agg(F.max("salary"))
    .orderBy("job_title")
)
