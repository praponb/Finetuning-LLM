from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNull() & F.col("location").isin(["Bangkok", "Singapore"]))
    .withColumn("location_prefix", F.substring("location", 1, 3))
    .withColumn("budget_filled", F.coalesce("budget", F.lit(0)))
)
