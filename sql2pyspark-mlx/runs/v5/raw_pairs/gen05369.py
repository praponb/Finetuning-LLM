from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Sales", "HR"]))
    .select(
        "id",
        "budget",
        F.round("budget", 1).alias("budget_rounded"),
        F.upper("dept_name").alias("dept_name_upper"),
    )
    .orderBy(F.col("budget_rounded").desc())
)
