from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Singapore", "Chiang Mai"]) & F.col("dept_name").isin(["HR", "Sales"]))
    .select(
        "dept_name",
        "budget",
        F.coalesce("dept_name", F.lit("unassigned")).alias("dept_name_filled"),
        F.round((F.col("budget") * 0.9), 2).alias("budget_scaled"),
    )
    .orderBy("dept_name")
)
