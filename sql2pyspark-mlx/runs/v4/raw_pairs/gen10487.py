from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") <= 1846000) & F.col("budget").isNotNull() & (F.col("location") != "Bangkok"))
    .select(
        "location",
        "dept_name",
        F.concat("location", F.lit("/"), "dept_name").alias("location_dept_name"),
        F.upper("dept_name").alias("dept_name_upper"),
    )
)
