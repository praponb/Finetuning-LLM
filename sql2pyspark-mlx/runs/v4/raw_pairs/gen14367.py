from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 3642000) & (F.col("location") == "New York"))
    .select(
        "id",
        "dept_name",
        "location",
        F.when(F.col("budget") >= 4135000, "High").when(F.col("budget") >= 1317000, "Medium").otherwise("Low").alias("budget_level"),
    )
)
