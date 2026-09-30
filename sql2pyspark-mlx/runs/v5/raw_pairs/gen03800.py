from pyspark.sql import functions as F

recent_employees = df_employees.filter((F.length("email") > 7) & (F.col("level") == "L5"))

employees_stats = (
    recent_employees
    .groupBy("is_remote")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = (
    employees_stats
    .filter(F.col("stddev_salary") < 45)
    .select("is_remote", "stddev_salary")
)
