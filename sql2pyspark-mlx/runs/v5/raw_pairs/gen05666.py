from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("location") != "New York"))
    .select(
        "*",
        F.trim("location").alias("clean_location"),
        F.concat("location", F.lit(" - "), "dept_name").alias("location_dept_name"),
        F.coalesce("location", F.lit("N/A")).alias("resolved_location"),
    )
)

result = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.job_title").isin(["Director", "Engineer", "Designer"])), "left_semi")
)
