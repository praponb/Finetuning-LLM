from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Berlin", "Bangkok"]) & (F.col("budget") <= 2268000))
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.when(F.col("dept_name") == "Legal", 1).otherwise(0).alias("is_legal"),
        F.round((F.col("budget") * 100), 2).alias("adjusted_budget"),
    )
)
