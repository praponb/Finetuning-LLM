from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "location",
        "budget",
        F.when(F.col("location").isin(["London", "Chiang Mai"]), "Priority").otherwise("Other").alias("location_group"),
    )
    .orderBy(F.col("location_group").desc())
)
