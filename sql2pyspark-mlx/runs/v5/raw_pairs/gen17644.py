from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isNull())
    .groupBy("level")
    .pivot("job_title", ["Engineer", "Manager"])
    .agg(F.max("salary"))
    .orderBy("level")
)
