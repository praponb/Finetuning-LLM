from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNull())
    .select(
        "id",
        "budget",
        "dept_name",
        F.round((F.col("budget") * 0.9), 2).alias("budget_scaled"),
    )
)
