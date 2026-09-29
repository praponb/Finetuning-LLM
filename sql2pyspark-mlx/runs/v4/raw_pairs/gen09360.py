from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(3289000, 3908000) | (F.col("budget") >= 3279000))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.initcap(F.trim("location")).alias("location_title"),
    )
)
