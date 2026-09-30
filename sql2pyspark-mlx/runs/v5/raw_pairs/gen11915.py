from pyspark.sql import functions as F

src = df_stores.filter(F.col("region").isin(["South", "East"]) & (F.col("sqft") == 15454))

summary = (
    src
    .groupBy("city", "region")
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
)

result = (
    summary
    .select("city", "region", "sqft_range")
    .orderBy("region")
)
