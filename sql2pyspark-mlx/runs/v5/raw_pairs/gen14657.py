from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Marketing") & (F.col("budget") > 3827000))
    .select(
        "budget",
        "dept_name",
        "location",
        F.upper("dept_name").alias("dept_name_upper"),
    )
)
