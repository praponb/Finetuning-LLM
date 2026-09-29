from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .pivot("location", ["New York", "London"])
    .agg(F.avg("budget").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "dept_name",
        F.col("New York_amount").alias("new_york_amount"),
        F.col("New York_n").alias("new_york_n"),
        F.col("London_amount").alias("london_amount"),
        F.col("London_n").alias("london_n"),
    )
)
