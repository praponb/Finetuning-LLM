from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2025-03-11", "2025-09-01") & F.month("opened_date").isin([1, 3, 9]) & F.col("store_name").isNotNull())
    .select(
        "id",
        "region",
        "sqft",
        F.concat("city", F.lit(", "), "region").alias("city_region"),
    )
)
