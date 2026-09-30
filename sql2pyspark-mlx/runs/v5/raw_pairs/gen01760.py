from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("full_name").like("Da%"))
    .select(
        "full_name",
        F.lower("level").alias("level_lower"),
        F.coalesce("level", F.lit("N/A")).alias("level_filled"),
    )
)
