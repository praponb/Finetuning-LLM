from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2024-01-01", "2025-03-11") & (F.col("store_name").like("Sia%") | (F.col("city") != "Chiang Mai")))
    .groupBy(
        F.when(F.col("region").isin(["East", "North"]), "tier_1").otherwise("other").alias("region_group"),
    )
    .agg(F.min("sqft").alias("min_sqft"))
)
