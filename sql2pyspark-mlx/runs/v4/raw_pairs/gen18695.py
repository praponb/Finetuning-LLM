from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").isNull() & F.col("email").like("%@startup.io"))
    .groupBy("is_remote", "level")
    .agg(
        F.count("*").alias("row_count"),
        F.avg("salary").alias("avg_salary"),
    )
)
