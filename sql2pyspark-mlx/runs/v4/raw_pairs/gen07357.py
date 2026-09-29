from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Legal", "Finance"]) | (F.col("dept_name") == "Marketing"))
    .select(
        "id",
        "dept_name",
        "budget",
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
    )
)
