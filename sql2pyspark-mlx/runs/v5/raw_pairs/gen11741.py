from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("location").isNotNull())
    .select(
        "*",
        F.concat_ws(",", "location", "dept_name").alias("location_dept_name_key"),
        F.coalesce("location", F.lit("DIRECT")).alias("location_clean"),
    )
)

result = (
    valid
    .groupBy("dept_name")
    .pivot("location", ["Singapore", "Berlin"])
    .agg(F.max("budget").alias("total"))
    .select(
        "dept_name",
        F.col("Singapore").alias("singapore"),
        F.col("Berlin").alias("berlin"),
    )
)
