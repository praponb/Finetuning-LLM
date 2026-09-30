from pyspark.sql import functions as F

src = df_employees.filter(F.col("email").like("%@gmail.com"))

summary = (
    src
    .groupBy("is_remote")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.min("salary").alias("min_salary"),
    )
)

result = (
    summary
    .filter(F.col("min_salary") < 120000)
    .select("is_remote", "avg_salary", "min_salary")
    .orderBy(F.col("avg_salary").desc())
)
