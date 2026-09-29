from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Tokyo") & (F.col("budget") >= 2629000))
    .select(
        "location",
        "dept_name",
        "budget",
        F.when(F.col("location").isin(["Bangkok", "Chiang Mai"]), "Tier 1").otherwise("Other").alias("location_group"),
    )
)
