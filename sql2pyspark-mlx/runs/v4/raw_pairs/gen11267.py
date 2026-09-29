from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") <= 2027000) & (F.col("budget") >= 2061000))
    .select(
        "id",
        "budget",
        "dept_name",
        "location",
        F.concat_ws(",", "location", "dept_name").alias("location_dept_name_key"),
        F.trim("location").alias("clean_location"),
    )
)
