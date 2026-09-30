from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 2291000)
    .select(
        "budget",
        "dept_name",
        "location",
        F.length("location").alias("location_length"),
    )
    .orderBy("budget", F.col("location_length").desc())
)
