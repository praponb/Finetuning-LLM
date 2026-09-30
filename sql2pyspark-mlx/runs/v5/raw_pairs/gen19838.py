from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("level").isin(["L3", "L5"]))
    .select("job_title")
    .distinct()
    .orderBy("job_title")
)
