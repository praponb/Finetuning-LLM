from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Engineering") & F.col("dept_name").isin(["HR", "Sales"]))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
    .withColumn("location_upper", F.upper("location"))
)
