from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("location") == "London")
    .select(
        "*",
        F.coalesce("dept_name", F.lit("Unknown")).alias("dept_name_filled"),
        F.trim("location").alias("clean_location"),
    )
)

result = (
    cleaned
    .groupBy("location")
    .pivot("dept_name", ["Sales", "HR"])
    .agg(F.sum("budget").alias("total"))
)
