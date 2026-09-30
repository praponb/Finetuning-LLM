from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("location") != "Tokyo"))
    .select(
        "*",
        F.floor("budget").alias("budget_whole"),
        F.concat_ws("-", "location", "dept_name").alias("location_dept_name_key"),
    )
)

result = (
    filtered
    .select("location", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("budget").between(2788000, 3739000))
        .select("location", "dept_name")
    )
)
