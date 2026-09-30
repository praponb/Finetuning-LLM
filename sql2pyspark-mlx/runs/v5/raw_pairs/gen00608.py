from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 2354000) & (F.col("dept_name") == "HR"))
    .groupBy("location")
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.count("*").alias("cnt"),
    )
)
