from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["New York", "Berlin"]) & F.col("location").isNull())
    .select(
        "dept_name",
        "budget",
        "location",
        F.lower("dept_name").alias("dept_name_lower"),
    )
)
