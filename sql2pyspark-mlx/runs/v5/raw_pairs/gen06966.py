from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(~F.col("dept_name").eqNullSafe(F.lit("Engineering")))
    .withColumn("location_lower", F.lower("location"))
    .withColumn("dept_name_location", F.concat("dept_name", F.lit(" - "), "location"))
)

result = (
    valid
    .groupBy("dept_name")
    .pivot("location", ["Singapore", "New York"])
    .agg(F.max("budget").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "dept_name",
        F.col("Singapore_amount").alias("singapore_amount"),
        F.col("Singapore_n").alias("singapore_n"),
        F.col("New York_amount").alias("new_york_amount"),
        F.col("New York_n").alias("new_york_n"),
    )
)
