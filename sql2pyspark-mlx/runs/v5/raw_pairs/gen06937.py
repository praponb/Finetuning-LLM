from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 1707000)
    .select(
        "dept_name",
        "budget",
        "location",
        F.round((F.col("budget") * 100), 2).alias("adjusted_budget"),
        F.lower("dept_name").alias("dept_name_lower"),
    )
    .orderBy("dept_name_lower")
)
