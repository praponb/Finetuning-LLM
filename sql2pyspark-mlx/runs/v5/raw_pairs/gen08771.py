from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 3791000)
    .select(
        "dept_name",
        F.concat("location", F.lit(" - "), "dept_name").alias("location_dept_name"),
    )
    .orderBy(F.col("location_dept_name").desc())
)
