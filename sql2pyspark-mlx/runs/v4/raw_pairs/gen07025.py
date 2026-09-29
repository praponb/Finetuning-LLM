from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("budget") >= 2390000)
    .select(
        "*",
        F.upper("location").alias("location_upper"),
        F.coalesce("location", F.lit("N/A")).alias("resolved_location"),
    )
)

result = (
    valid
    .groupBy("location")
    .pivot("dept_name", ["HR", "Marketing"])
    .agg(F.sum("budget").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
