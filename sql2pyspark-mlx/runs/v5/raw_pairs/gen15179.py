from pyspark.sql import functions as F

base = df_stores.filter((F.col("city") == "London") & (F.col("sqft") == 5037))

stores_stats = (
    base
    .groupBy("city")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.count("sqft").alias("sqft_count"),
    )
)

result = stores_stats.orderBy("city")
