from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Singapore") & (F.col("budget") <= 3050000))
    .select(
        "id",
        "budget",
        F.trim("location").alias("clean_location"),
        F.when(F.col("location").isin(["Bangkok", "New York"]), "Priority").otherwise("Other").alias("location_group"),
    )
    .orderBy("location_group")
)
