from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter((F.year("opened_date") >= 2023) & F.col("opened_date").between("2023-10-01", "2025-01-01"))
)

stores_stats = (
    filtered_stores
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "West", 1).otherwise(0))) / F.count("*")), 1).alias("pct_west"),
    )
)

result = (
    stores_stats
    .select("city", "pct_west")
    .orderBy(F.col("pct_west").desc())
)
