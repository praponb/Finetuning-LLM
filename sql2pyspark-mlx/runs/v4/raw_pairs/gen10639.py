from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .pivot("job_title", ["Intern", "Manager", "Analyst", "Designer"])
    .agg(F.sum("salary"))
    .orderBy("level")
)
