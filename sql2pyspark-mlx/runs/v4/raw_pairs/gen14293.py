from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.lower(F.trim("full_name")) == "carol diaz")
    .groupBy("job_title")
    .pivot("level", ["L3", "L1"])
    .agg(F.sum("salary"))
)
