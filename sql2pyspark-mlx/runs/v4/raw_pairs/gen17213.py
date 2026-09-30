from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 2287000)
    .select("location", "dept_name")
    .unionByName(
        df_departments
        .filter(F.col("location") != "Singapore")
        .select("location", "dept_name")
    )
    .distinct()
)
