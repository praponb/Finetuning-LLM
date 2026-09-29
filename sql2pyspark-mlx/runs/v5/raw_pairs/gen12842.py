from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & F.col("dept_name").isin(["Finance", "HR", "Marketing"]))
    .withColumn("location_dept_name", F.concat("location", F.lit("/"), "dept_name"))
)

result = (
    cleaned
    .groupBy("dept_name")
    .pivot("location", ["Tokyo", "Chiang Mai"])
    .agg(F.avg("budget"))
    .select(
        "dept_name",
        F.col("Tokyo").alias("tokyo"),
        F.col("Chiang Mai").alias("chiang_mai"),
    )
)
