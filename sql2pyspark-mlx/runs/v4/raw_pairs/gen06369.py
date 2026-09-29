from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Berlin")
    .select(
        "id",
        "dept_name",
        F.concat("dept_name", F.lit(" - "), "location").alias("dept_name_location"),
    )
)
