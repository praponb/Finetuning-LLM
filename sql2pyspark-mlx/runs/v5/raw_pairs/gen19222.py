from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "budget",
        "location",
        "dept_name",
        F.concat("location", F.lit(", "), "dept_name").alias("location_dept_name"),
    )
)
