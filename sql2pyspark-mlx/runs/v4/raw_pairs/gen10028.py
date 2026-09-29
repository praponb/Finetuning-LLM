from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Marketing", "HR"]))
    .groupBy("location")
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
    .orderBy(F.col("stddev_budget").desc())
)
