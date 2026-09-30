from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "New York") & (F.col("location") != "Bangkok"))
    .select(
        "id",
        "dept_name",
        F.concat("location", F.lit("/"), "dept_name").alias("location_dept_name"),
    )
)
