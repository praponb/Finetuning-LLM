from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Sales") & F.col("budget").isNotNull())
    .select(
        "dept_name",
        F.concat("dept_name", F.lit(" "), "location").alias("dept_name_location"),
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
    )
)
