from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("dept_id") == 13) & F.col("level").isin(["L1", "L4", "L3"]))
    .groupBy("level", "job_title")
    .agg(
        F.max("hire_date").alias("last_hire_date"),
        F.countDistinct("salary").alias("unique_levels"),
    )
    .filter(F.col("unique_levels") > 5)
    .orderBy(F.col("level").desc())
)
