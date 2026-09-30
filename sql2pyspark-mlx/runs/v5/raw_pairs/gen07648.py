from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .pivot("job_title", ["Designer", "Director", "Analyst"])
    .agg(F.max("salary"))
)
