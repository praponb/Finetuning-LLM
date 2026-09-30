from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["Berlin", "Bangkok"])
    .agg(F.avg("budget").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "dept_name",
        F.col("Berlin_value_sum").alias("berlin_value_sum"),
        F.col("Berlin_cnt").alias("berlin_cnt"),
        F.col("Bangkok_value_sum").alias("bangkok_value_sum"),
        F.col("Bangkok_cnt").alias("bangkok_cnt"),
    )
)
