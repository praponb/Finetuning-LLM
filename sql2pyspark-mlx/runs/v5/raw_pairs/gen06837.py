from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("dept_name").isNull())
    .select(
        "*",
        F.when(F.col("budget") >= 3984000, "High").when(F.col("budget") >= 1796000, "Medium").otherwise("Low").alias("budget_tier"),
    )
)

result = (
    base
    .select("location", "dept_name")
    .subtract(
        df_departments
        .filter(F.col("budget") > 3108000)
        .select("location", "dept_name")
    )
)
