from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
)
