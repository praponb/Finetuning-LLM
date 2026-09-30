from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Sales") & F.col("dept_name").isin(["Marketing", "Engineering", "Sales"]))
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.when(F.col("budget") >= 3162000, "High").when(F.col("budget") >= 664000, "Medium").otherwise("Low").alias("budget_level"),
    )
)
