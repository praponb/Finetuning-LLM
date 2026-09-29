from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "budget",
        "dept_name",
        "location",
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
    .orderBy("dept_name")
)
