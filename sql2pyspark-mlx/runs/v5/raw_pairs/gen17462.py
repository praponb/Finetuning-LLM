from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.year("opened_date") >= 2025)
    .withColumn("region_upper", F.upper("region"))
)

summary = (
    base
    .groupBy("region")
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
)

result = (
    summary
    .filter(F.col("sqft_range") > 28)
    .select("region", "sqft_range")
    .orderBy(F.col("region").desc(), "sqft_range")
)
