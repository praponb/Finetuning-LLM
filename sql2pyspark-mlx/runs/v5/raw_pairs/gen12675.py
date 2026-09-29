from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([22, 28]) & (F.length("full_name") > 10))
    .groupBy("is_remote", "level")
    .agg(F.sum("salary").alias("total_salary"))
    .filter(F.col("total_salary") >= 788000)
    .orderBy(F.col("level").desc())
)
