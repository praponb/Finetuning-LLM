from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Berlin") & (F.col("budget") <= 3417000))
    .select(
        "id",
        "budget",
        "location",
        "dept_name",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.when(F.col("dept_name") == "Marketing", 1).otherwise(0).alias("is_marketing"),
    )
)
