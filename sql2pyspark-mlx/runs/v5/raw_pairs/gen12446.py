from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") <= "2025-05-18") & F.col("city").isin(["Berlin", "Tokyo", "New York"]))
    .select(
        "id",
        "opened_date",
        "region",
        "sqft",
        F.coalesce("store_name", F.lit("N/A")).alias("store_name_filled"),
    )
    .orderBy(F.col("sqft").desc())
)
