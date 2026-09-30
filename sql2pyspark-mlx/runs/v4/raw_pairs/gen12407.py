from pyspark.sql import functions as F

base = df_stores.filter(F.col("region") == "North")

region_summary = (
    base
    .groupBy("region")
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
)

result = region_summary.orderBy("region")
