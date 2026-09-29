from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Marketing"]) & (F.col("location") == "Tokyo") & ~F.col("location").isin(["Berlin", "New York", "London"]))
    .select(
        "id",
        "dept_name",
        "location",
        F.round((F.col("budget") * 1.07), 2).alias("budget_scaled"),
        F.initcap(F.trim("dept_name")).alias("dept_name_title"),
    )
    .orderBy("dept_name")
)
