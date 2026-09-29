from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.round("budget", 0).alias("budget_rounded"),
    )
    .orderBy("budget_rounded", "id")
    .limit(10)
)
