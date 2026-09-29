from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 1585000)
    .select(
        "id",
        "budget",
        "location",
        F.coalesce("location", F.lit("unassigned")).alias("location_filled"),
        F.when(F.col("budget") >= 3845000, "High").when(F.col("budget") >= 1895000, "Medium").otherwise("Low").alias("budget_level"),
    )
    .orderBy("budget")
)
