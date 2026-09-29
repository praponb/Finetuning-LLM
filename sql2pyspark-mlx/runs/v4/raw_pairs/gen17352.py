from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("dept_id") == 6)
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
)

employees_stats = (
    src
    .groupBy("level")
    .agg(
        F.max("salary").alias("max_salary"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)

result = (
    employees_stats
    .filter(F.col("avg_salary") < 40)
    .select("level", "max_salary", "avg_salary")
    .orderBy("avg_salary", "level")
)
