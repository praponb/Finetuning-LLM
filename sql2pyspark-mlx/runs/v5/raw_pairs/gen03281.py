from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level", "manager_id")
    .agg(
        F.sum("salary").alias("total_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)
