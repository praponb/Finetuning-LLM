from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Tokyo") | F.col("dept_name").isNotNull())
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.concat_ws(",", "dept_name", "location").alias("dept_name_location_key"),
    )
    .orderBy("dept_name_location_key", "id")
    .limit(50)
)
