from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1417000, 1854000))
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.round((F.col("budget") * 1.07), 2).alias("budget_adjusted"),
    )
    .orderBy(F.col("location").desc())
)
