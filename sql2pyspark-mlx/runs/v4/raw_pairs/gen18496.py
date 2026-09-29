from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Chiang Mai", "Tokyo", "Singapore"]) & (F.col("budget") >= 3286000))
    .groupBy(
        F.when(F.col("location").isin(["Singapore", "Chiang Mai"]), "core").otherwise("other").alias("location_group"),
        "location",
    )
    .agg(F.countDistinct("budget").alias("unique_locations"))
    .orderBy(F.col("location_group").desc())
)
