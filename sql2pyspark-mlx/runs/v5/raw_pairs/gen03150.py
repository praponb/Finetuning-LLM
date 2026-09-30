from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") >= 3144000)
    .select(
        "location",
        "dept_name",
        "budget",
        F.round((F.col("budget") * 1.15), 2).alias("budget_scaled"),
    )
)
