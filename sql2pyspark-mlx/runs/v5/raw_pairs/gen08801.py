from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Marketing") & (F.col("budget") > 1087000))
    .select(
        "budget",
        "dept_name",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
    )
)
