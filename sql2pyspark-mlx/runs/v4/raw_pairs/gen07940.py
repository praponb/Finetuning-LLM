from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["Singapore", "Bangkok"])
    .agg(F.max("budget").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "dept_name",
        F.col("Singapore_total").alias("singapore_total"),
        F.col("Singapore_cnt").alias("singapore_cnt"),
        F.col("Bangkok_total").alias("bangkok_total"),
        F.col("Bangkok_cnt").alias("bangkok_cnt"),
    )
)
