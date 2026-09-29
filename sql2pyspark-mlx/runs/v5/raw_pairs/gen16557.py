from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(~F.col("location").eqNullSafe(F.lit("New York")))
    .select(
        "*",
        F.length("dept_name").alias("dept_name_length"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("dept_name_filled"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.dept_id").isin([2, 37])), "left_anti")
)
