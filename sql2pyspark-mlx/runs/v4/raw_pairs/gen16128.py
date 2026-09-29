from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "Berlin"]) & F.col("location").isin(["London", "Berlin", "Bangkok"]) & F.col("budget").between(3153000, 3608000))
    .select(
        "dept_name",
        "location",
        "budget",
        F.initcap(F.trim("location")).alias("location_title"),
        F.round("budget", 0).alias("budget_rounded"),
    )
)
