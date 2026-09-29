from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 2582000)
    .select(
        "id",
        "budget",
        "dept_name",
        F.when(F.col("budget") >= 4047000, "High").when(F.col("budget") >= 1524000, "Medium").otherwise("Low").alias("budget_level"),
    )
    .orderBy(F.col("budget").desc(), F.col("budget_level").desc(), "id")
    .limit(1)
)
