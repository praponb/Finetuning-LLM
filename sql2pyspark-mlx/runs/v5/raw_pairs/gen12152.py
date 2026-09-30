from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.month("opened_date").isin([2, 3, 7]) & F.col("city").isin(["Singapore", "Chiang Mai"]))
    .groupBy("region")
    .agg(
        F.max("sqft").alias("max_sqft"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
    .orderBy("sqft_range")
)
