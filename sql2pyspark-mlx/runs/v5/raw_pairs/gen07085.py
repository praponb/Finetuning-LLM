from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(~F.col("dept_name").isin(["Marketing", "Legal"]))
    .withColumn("location_length", F.length("location"))
)

result = (
    cleaned
    .select("id", "dept_name")
    .intersect(
        df_departments
        .filter(F.col("dept_name").isin(["Legal", "Marketing"]))
        .select("id", "dept_name")
    )
)
