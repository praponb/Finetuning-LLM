from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["London", "Tokyo"])
    .agg(F.max("budget").alias("value_sum"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "dept_name",
        F.col("London_value_sum").alias("london_value_sum"),
        F.col("London_cnt").alias("london_cnt"),
        F.col("Tokyo_value_sum").alias("tokyo_value_sum"),
        F.col("Tokyo_cnt").alias("tokyo_cnt"),
    )
)
