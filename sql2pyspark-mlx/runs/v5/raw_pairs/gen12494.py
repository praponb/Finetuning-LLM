from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isin(["London", "Singapore"]))
    .withColumn("location_dept_name", F.concat("location", F.lit(", "), "dept_name"))
)

result = (
    base
    .groupBy("location")
    .pivot("dept_name", ["Finance", "Marketing"])
    .agg(F.sum("budget"))
    .select(
        "location",
        F.col("Finance").alias("finance"),
        F.col("Marketing").alias("marketing"),
    )
)
