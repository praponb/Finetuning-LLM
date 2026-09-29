from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Tokyo", "Singapore"]) & F.col("dept_name").isin(["HR", "Legal", "Marketing"]))
    .select(
        "id",
        "location",
        F.when(F.col("budget") >= 4390000, "High").when(F.col("budget") >= 1856000, "Medium").otherwise("Low").alias("budget_level"),
    )
)
