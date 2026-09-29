from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("location").isNotNull())
    .withColumn("location_title", F.initcap(F.trim("location")))
    .withColumn("dept_name_prefix", F.substring("dept_name", 1, 2))
)

result = (
    filtered
    .select("location")
    .intersectAll(
        df_departments
        .filter(F.col("location").isin(["New York", "Berlin", "London"]))
        .select("location")
    )
)
