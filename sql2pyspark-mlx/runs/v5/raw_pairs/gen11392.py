from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Tokyo", "New York"]))
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.concat_ws(",", "location", "dept_name").alias("location_dept_name_key"),
        F.concat("location", F.lit("/"), "dept_name").alias("location_dept_name"),
    )
)
