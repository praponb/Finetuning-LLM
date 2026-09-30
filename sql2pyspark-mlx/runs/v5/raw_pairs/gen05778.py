from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Bangkok", "Chiang Mai", "Singapore"]))
    .select(
        "id",
        "dept_name",
        "location",
        F.coalesce("budget", F.lit(0)).alias("budget_filled"),
        F.trim("location").alias("clean_location"),
    )
)
