from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("full_name") < 12)
    .groupBy("level", "is_remote")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.avg("salary").alias("avg_salary"),
    )
    .orderBy(F.col("avg_salary").desc(), F.col("level").desc(), "is_remote")
    .limit(10)
)
