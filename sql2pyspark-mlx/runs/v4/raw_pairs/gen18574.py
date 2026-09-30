from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Bangkok")
    .groupBy(F.year("opened_date").alias("opened_year"), "city")
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.count("sqft").alias("sqft_count"),
    )
    .filter(F.col("sqft_count") > 3)
    .drop("sqft_count")
    .orderBy(F.col("sqft_range").desc(), "east_count")
)
