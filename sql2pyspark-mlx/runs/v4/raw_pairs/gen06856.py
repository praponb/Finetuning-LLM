from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Finance", "HR"]) & (F.col("budget") > 3940000))
    .select(
        "location",
        "dept_name",
        F.round((F.col("budget") * 100), 2).alias("adjusted_budget"),
        F.when(F.col("budget") >= 4247000, "High").when(F.col("budget") >= 2043000, "Medium").otherwise("Low").alias("budget_band"),
    )
)
