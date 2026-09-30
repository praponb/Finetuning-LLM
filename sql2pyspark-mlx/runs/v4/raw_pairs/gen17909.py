from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~((F.col("dept_name") != "Engineering") & F.col("dept_name").isin(["Legal", "Engineering"]) & ~F.col("location").isin(["Berlin", "Tokyo", "Singapore"])))
    .select(
        "id",
        "budget",
        "dept_name",
        F.substring("dept_name", 1, 2).alias("dept_name_prefix"),
    )
)
