from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.max("hire_date").alias("last_hire_date"),
    )
    .filter(F.col("avg_salary") >= 76)
    .select("level", "avg_salary", "last_hire_date")
    .orderBy(F.col("level").desc(), F.col("last_hire_date").desc())
)
