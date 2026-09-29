from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Bangkok"]))
    .select(
        "*",
        F.round((F.col("budget") * 0.9), 2).alias("adjusted_budget"),
        F.concat_ws("-", "dept_name", "location").alias("dept_name_location_key"),
        F.coalesce("location", F.lit("OTHER")).alias("location_filled"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date").between("2023-05-01", "2024-01-25")), "left_anti")
)
