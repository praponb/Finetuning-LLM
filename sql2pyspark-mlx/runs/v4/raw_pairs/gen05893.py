from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Marketing") | F.col("dept_name").isNull())
    .select(
        "id",
        "budget",
        "location",
        "dept_name",
        F.concat_ws("-", "location", "dept_name").alias("location_dept_name_key"),
    )
)
