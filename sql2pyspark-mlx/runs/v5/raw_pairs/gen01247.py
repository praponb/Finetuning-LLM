from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Marketing") & (F.col("dept_name").isin(["Marketing", "HR", "Legal"]) | F.col("location").isin(["New York", "London"])))
    .select(
        "id",
        "dept_name",
        "location",
        F.concat_ws("-", "location", "dept_name").alias("location_dept_name_key"),
    )
)
