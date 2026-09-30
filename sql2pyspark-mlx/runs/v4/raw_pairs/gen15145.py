from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["Berlin", "Tokyo", "Singapore"]))
    .select(
        "dept_name",
        "location",
        F.initcap(F.trim("location")).alias("location_title"),
    )
)
