from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@yahoo.com"))
    .groupBy("level")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.countDistinct("salary").alias("unique_depts"),
    )
    .orderBy("level")
)
