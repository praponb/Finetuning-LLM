from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "budget",
        "dept_name",
        F.concat("location", F.lit("/"), "dept_name").alias("location_dept_name"),
        F.when(F.col("location") == "New York", 1).otherwise(0).alias("is_new_york"),
    )
)
