from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["New York", "Singapore", "Tokyo"]) & (F.col("dept_name") == "Engineering"))
    .select(
        "dept_name",
        "budget",
        "location",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
    )
    .orderBy(F.col("budget_filled").desc(), "location")
)
