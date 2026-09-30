from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Berlin") & (F.col("dept_name").isin(["Engineering", "Legal", "Sales"]) | (F.col("location") == "London")))
    .select(
        "dept_name",
        "location",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.trim("location").alias("clean_location"),
    )
)
