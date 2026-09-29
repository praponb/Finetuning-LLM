from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "London"]))
    .select(
        "*",
        F.length("location").alias("location_length"),
        F.coalesce("dept_name", F.lit("OTHER")).alias("dept_name_filled"),
    )
)

result = (
    valid
    .select("location", "dept_name")
    .intersect(
        df_departments
        .filter(F.col("location") == "New York")
        .select("location", "dept_name")
    )
)
