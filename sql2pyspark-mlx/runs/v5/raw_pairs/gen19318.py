from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") <= 2147000) & F.col("budget").between(2668000, 3717000))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.initcap(F.trim("location")).alias("location_title"),
        F.ceil("budget").alias("budget_whole"),
    )
)
