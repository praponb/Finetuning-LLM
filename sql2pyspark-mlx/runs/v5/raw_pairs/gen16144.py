from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2298000, 3296000))
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.when(F.col("dept_name") == "HR", 1).otherwise(0).alias("is_hr"),
    )
    .orderBy("budget", "dept_name", "id")
    .limit(10)
)
