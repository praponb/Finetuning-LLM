from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(3337000, 3590000))
    .select(
        "id",
        "budget",
        F.concat_ws("-", "location", "dept_name").alias("location_dept_name_key"),
    )
    .orderBy("budget", "id")
    .limit(20)
)
