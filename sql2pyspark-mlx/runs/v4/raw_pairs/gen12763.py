from pyspark.sql import functions as F

result = (
    df_departments
    .filter(((F.col("budget") < 2406000) & F.col("dept_name").isin(["Finance", "Legal", "Sales"])) | F.col("budget").between(2874000, 3759000))
    .select(
        "location",
        "dept_name",
        "budget",
        F.trim("dept_name").alias("clean_dept_name"),
    )
)
