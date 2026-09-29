from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "HR"]))
    .select(
        "id",
        "dept_name",
        F.length("dept_name").alias("dept_name_length"),
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
)
