from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.initcap(F.trim("location")).alias("location_title"),
        F.coalesce("location", F.lit("DIRECT")).alias("location_filled"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.is_remote")), "left_anti")
)
