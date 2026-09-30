from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["London", "Chiang Mai", "Berlin"]) & F.col("dept_name").isin(["Legal", "Sales", "Finance"]))
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.sum(F.when(F.col("dept_name") == "Legal", 1).otherwise(0)).alias("legal_count"),
        F.sum(F.when(F.col("dept_name") == "HR", 1).otherwise(0)).alias("hr_count"),
    )
)
