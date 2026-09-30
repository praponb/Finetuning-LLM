from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.col("opened_date").between("2023-10-01", "2024-06-05"))

stores_stats = (
    valid_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
    )
)

result = stores_stats
